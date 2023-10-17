import os
import re
import sys
import ssl
import time
import uuid
import json
import shutil
import asyncio
import logging
import argparse
import commitlog
import traceback
import urllib.parse
from logging import critical as log


def path_join(*path):
    return os.path.join(*[str(p) for p in path])


def seq2path(log_seq):
    return path_join(G.logdir, log_seq//100000, log_seq//1000, log_seq)


def dump(path, *objects):
    if os.path.dirname(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp = path + '.' + str(uuid.uuid4()) + '.tmp'
    with open(tmp, 'wb') as fd:
        for obj in objects:
            if type(obj) is not bytes:
                obj = json.dumps(obj, sort_keys=True).encode()

            fd.write(obj)

    os.replace(tmp, path)


async def fetch(log_seq, what):
    path = seq2path(int(log_seq))
    if os.path.isfile(path):
        with open(path, 'rb') as fd:
            return fd.read() if 'body' == what else json.loads(fd.readline())


# PROMISE - Block stale leaders and return the most recent accepted value.
# Client will propose the most recent across servers in the accept phase
async def paxos_promise(proposal_seq):
    proposal_seq = int(proposal_seq)

    with open(G.promise_filepath) as fd:
        promised_seq = json.load(fd)['promised_seq']

    # proposal_seq has to be strictly bigger than whatever seen so far
    if proposal_seq > promised_seq:
        dump(G.promise_filepath, dict(promised_seq=proposal_seq))
        os.sync()

        if G.max_seq > 0:
            with open(seq2path(G.max_seq), 'rb') as fd:
                return fd.read()

        hdr = dict(log_seq=0, accepted_seq=0)
        return json.dumps(hdr, sort_keys=True).encode() + b'\n'


# ACCEPT - Client has sent the most recent value from the promise phase.
# Stale leaders blocked. Only the most recent can reach this stage.
async def paxos_accept(proposal_seq, log_seq, commit_id, octets):
    log_seq = int(log_seq)
    proposal_seq = int(proposal_seq)

    with open(G.promise_filepath) as fd:
        promised_seq = json.load(fd)['promised_seq']

    # proposal_seq has to be bigger than or equal to the biggest seen so far
    if proposal_seq >= promised_seq:
        # Continuous cleanup - remove an older file before writing a new one
        # Retain only the most recent 1 million log records
        old_log_record = seq2path(log_seq - 1000*1000)
        if os.path.isfile(old_log_record):
            os.remove(old_log_record)

        # Record new proposal_seq as it is bigger than the current value.
        # Any future writes with a smaller seq would be rejected.
        if proposal_seq > promised_seq:
            dump(G.promise_filepath, dict(promised_seq=proposal_seq))

        hdr = dict(accepted_seq=proposal_seq, log_seq=log_seq,
                   cluster_id=G.cluster_id,
                   commit_id=commit_id, length=len(octets))

        if octets:
            dump(seq2path(log_seq), hdr, b'\n', octets)
            G.max_seq = max(G.max_seq, log_seq)
            os.sync()

            return hdr


def sorted_dir(dirname):
    return sorted([int(f) for f in os.listdir(dirname) if f.isdigit()])


async def delete(seq):
    seq = int(seq)

    count = 0
    for x in sorted_dir(G.logdir):
        for y in sorted_dir(path_join(G.logdir, x)):
            for f in sorted_dir(path_join(G.logdir, x, y)):
                if f > seq:
                    return count

                os.remove(path_join(G.logdir, x, y, f))
                count += 1

            shutil.rmtree(path_join(G.logdir, x, y))
        shutil.rmtree(path_join(G.logdir, x))


async def init():
    return await G.client.init()


async def write(octets):
    return await G.client.write(octets)


async def read(seq):
    hdr, octets = await G.client.read(seq)
    return json.dumps(hdr, sort_keys=True).encode() + b'\n' + octets


async def echo(msg):
    return dict(msg=msg)


class HTTPHandler():
    def __init__(self, methods):
        self.methods = methods

    async def __call__(self, reader, writer):
        peer = None
        count = 1

        while True:
            try:
                peer = writer.get_extra_info('socket').getpeername()

                line = await reader.readline()
                p = line.decode().split()[1].strip('/').split('/')

                method = p[0]
                params = {k.lower(): urllib.parse.unquote(v)
                          for k, v in zip(p[1::2], p[2::2])}

                length = 0
                while True:
                    line = await reader.readline()
                    line = line.strip()
                    if not line:
                        break
                    k, v = line.decode().split(':', maxsplit=1)
                    if 'content-length' == k.strip().lower():
                        length = int(v.strip())

                if length > 0:
                    params['octets'] = await reader.readexactly(length)
            except Exception:
                return writer.close()

            status = '400 Bad Request'
            mime_type = 'application/octet-stream'

            try:
                res = await self.methods[method](**params)
                if res is None:
                    res = b''
                else:
                    status = '200 OK'

                if type(res) is not bytes:
                    res = json.dumps(res, indent=4, sort_keys=True).encode()
                    mime_type = 'application/json'
            except Exception:
                traceback.print_exc()
                res = traceback.format_exc().encode()

            try:
                writer.write(f'HTTP/1.1 {status}\n'.encode())
                writer.write(f'content-length: {len(res)}\n'.encode())
                if res:
                    writer.write(f'content-type: {mime_type}\n\n'.encode())
                    writer.write(res)
                else:
                    writer.write(b'\n')
                await writer.drain()
            except Exception:
                return writer.close()

            params.pop('octets', None)
            log(f'{peer} {count} {method} {params} {length} {len(res)}')
            count += 1


async def server():
    G.promise_filepath = os.path.join(G.logdir, 'promised')

    if not os.path.isfile(G.promise_filepath):
        dump(G.promise_filepath, dict(promised_seq=0))

    handler = HTTPHandler(dict(
        echo=echo, fetch=fetch, init=init, read=read, write=write,
        delete=delete, promise=paxos_promise, commit=paxos_accept))

    ctx = commitlog.load_cert(G.cert, ssl.Purpose.CLIENT_AUTH)
    srv = await asyncio.start_server(handler, None, G.port, ssl=ctx)

    async with srv:
        return await srv.serve_forever()


async def cmd_init():
    if os.path.isfile(G.init):
        os.remove(G.init)

    ts = time.time()

    result = await G.client.init()
    if result is None:
        log('init failed')
        exit(1)

    dump(G.init, result)

    result['msec'] = int((time.time() - ts) * 1000)

    log(result)


async def cmd_write():
    if not os.path.isfile(G.write):
        log('incomplete init')
        exit(1)

    with open(G.write) as fd:
        obj = json.load(fd)

    os.remove(G.write)

    octets = sys.stdin.buffer.read(1024*1024)

    ts = time.time()

    await G.client.init(obj['proposal_seq'], obj['log_seq'])

    result = await G.client.write(octets)
    if not result:
        log('commit failed')
        exit(1)

    obj['log_seq'] = result['log_seq']
    dump(G.write, obj)

    result['msec'] = int((time.time() - ts) * 1000)

    log(result)


async def cmd_delete():
    log(await G.client.delete(G.delete))


async def cmd_tail():
    seq = G.max_seq + 1

    while True:
        result = await G.client.read(seq)
        if not result:
            await asyncio.sleep(1)
            continue

        hdr, octets = result

        path = seq2path(seq)
        dump(path, hdr, b'\n', octets)

        with open(path) as fd:
            log(fd.readline().strip())

        seq += 1


def reverse_sorted_dir(dirname):
    files = [int(f) for f in os.listdir(dirname) if f.isdigit()]
    return sorted(files, reverse=True)


def get_max_seq(logdir):
    # Traverse the three level directory hierarchy,
    # picking the highest numbered dir/file at each level
    for x in reverse_sorted_dir(logdir):
        for y in reverse_sorted_dir(path_join(logdir, x)):
            for f in reverse_sorted_dir(path_join(logdir, x, y)):
                return f

    return 0


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--init', help='filename to store leader state')
    G.add_argument('--delete', type=int, help='delete before this seq number')
    G.add_argument('--write', help='filename to store/read leader state')
    G.add_argument('--port', help='port number for server')
    G.add_argument('--cert', help='Self signed certificate file path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G = G.parse_args()

    ctx = commitlog.load_cert(G.cert, ssl.Purpose.CLIENT_AUTH)
    G.cluster_id = str(uuid.UUID(
        re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}',
                  ctx.get_ca_certs()[0]['subject'][0][0][1])[0]))
    G.logdir = os.path.join('commitlog', G.cluster_id)

    os.makedirs(G.logdir, exist_ok=True)
    G.max_seq = get_max_seq(G.logdir)

    if G.servers:
        G.client = commitlog.Client(G.cert, G.servers)

    if G.port:
        asyncio.run(server())
    elif G.init:
        asyncio.run(cmd_init())
    elif G.write:
        asyncio.run(cmd_write())
    elif G.delete:
        asyncio.run(cmd_delete())
    else:
        asyncio.run(cmd_tail())
