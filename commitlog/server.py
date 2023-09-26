import os
import re
import sys
import ssl
import json
import uuid
import time
import shutil
import hashlib
import asyncio
import logging
import traceback
import commitlog
from logging import critical as log


async def server(reader, writer):
    HANDLERS = dict(promise=paxos_server, accept=paxos_server,
                    logseq=logseq_server, read=read_server,
                    elect=paxos_client)

    peer = writer.get_extra_info('socket').getpeername()

    while True:
        try:
            try:
                req = await reader.readline()
                if not req or len(req) > 1024:
                    if req:
                        log(f'{peer} header too long {len(req)} > 1KB')
                    return writer.close()

                req = req.decode().strip()
                method, header, length = json.loads(req)
            except Exception:
                log(f'{peer} disconnected or invalid header')
                return writer.close()

            if method not in HANDLERS or length > 1024*1024:
                log(f'{peer} invalid request {req}')
                return writer.close()

            status, header, body = await HANDLERS[method](
                header, await reader.readexactly(length))

            length = len(body) if body else 0
            res = json.dumps([status, header, length])

            writer.write(res.encode())
            writer.write(b'\n')
            if length > 0:
                writer.write(body)

            await writer.drain()
            log(f'{peer} {method}:{status} {req} {res}')
        except Exception as e:
            traceback.print_exc()
            log(f'{peer} FATAL({e})')
            os._exit(0)


def path_join(*path):
    return os.path.join(*[str(p) for p in path])


def seq2path(log_seq):
    return path_join(G.logdir, log_seq//100000, log_seq//1000, log_seq)


def listdir(dirname):
    files = [int(f) for f in os.listdir(dirname) if f.isdigit()]
    return sorted(files, reverse=True)


def latest_logseq():
    # Traverse the three level directory hierarchy,
    # picking the highest numbered dir/file at each level
    for x in listdir(G.logdir):
        for y in listdir(path_join(G.logdir, x)):
            for f in listdir(path_join(G.logdir, x, y)):
                return f

    return 0


async def logseq_server(header, body):
    return 'OK', latest_logseq(), None


async def read_server(header, body):
    what, log_seq = header

    path = seq2path(log_seq)
    if not os.path.isfile(path):
        return 'NOTFOUND', None, None

    with open(path, 'rb') as fd:
        header = json.loads(fd.readline())

        return 'OK', header, fd.read() if 'body' == what else None


def dump(path, *objects):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp = os.path.join(G.logdir, str(uuid.uuid4()) + '.tmp')
    with open(tmp, 'wb') as fd:
        for obj in objects:
            if type(obj) is not bytes:
                obj = json.dumps(obj, sort_keys=True).encode()

            fd.write(obj)

    os.replace(tmp, path)
    os.sync()


async def paxos_server(header, body):
    phase = 'promise' if 1 == len(header) else 'accept'
    proposal_seq = header[0]

    promised_seq = 0
    promise_filepath = os.path.join(G.logdir, 'promised')
    if os.path.isfile(promise_filepath):
        with open(promise_filepath) as fd:
            promised_seq = json.load(fd)['promised_seq']

    # PROMISE - Block stale leaders and return the most recent accepted value.
    # Client will propose the most recent across servers in the accept phase
    if 'promise' == phase and proposal_seq > promised_seq:
        dump(promise_filepath, dict(promised_seq=proposal_seq))

        # Most recent log file
        log_seq = latest_logseq()

        # Log stream does not yet exist
        if 0 == log_seq:
            return 'OK', dict(log_seq=0, accepted_seq=0), None

        with open(seq2path(log_seq), 'rb') as fd:
            return 'OK', json.loads(fd.readline()), fd.read()

    # ACCEPT - Client has sent the most recent value from the promise phase.
    # Stale leaders blocked. Only the most recent can reach this stage.
    if 'accept' == phase and proposal_seq >= promised_seq:
        log_seq, commit_id = header[1], header[2]

        # Continuous cleanup - remove an older file before writing a new one
        # Retain only the most recent 1 million log records
        old_log_record = seq2path(log_seq - 1000*1000)
        if os.path.isfile(old_log_record):
            os.remove(old_log_record)
            log(f'removed old record log_seq({old_log_record})')

        if proposal_seq > promised_seq:
            dump(promise_filepath, dict(promised_seq=proposal_seq))

        header = dict(accepted_seq=proposal_seq, log_id=G.log_id,
                      log_seq=log_seq, commit_id=commit_id,
                      length=len(body), sha1=hashlib.sha1(body).hexdigest())

        dump(seq2path(header['log_seq']), header, b'\n', body)

        return 'OK', header, None

    return 'STALE_PROPOSAL_SEQ', None, None


# Used for leader election - Ideally, this should be part of the client code.
# However, since leader election is done infrequently, its more maintainable to
# keep this code here and call it over RPC. This makes client very lightweight.
async def paxos_client(header, body):
    cert, servers = sys.argv[1], header

    rpc = commitlog.RPC(cert, servers)
    quorum = int(len(servers)/2) + 1
    proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

    # paxos PROMISE phase - block stale leaders from writing
    res = await rpc('promise', [proposal_seq])
    if quorum > len(res):
        return 'NO_PROMISE_QUORUM', None, None

    headers = {json.dumps(h, sort_keys=True) for h, _ in res.values()}
    if 1 == len(headers):
        log_seq = json.loads(headers.pop())['log_seq']
        return 'OK', [proposal_seq, log_seq + 1], None

    # This is the CRUX of the paxos protocol
    # Find the most recent log_seq with most recent accepted_seq
    # Only this value should be proposed, else everything breaks
    log_seq = accepted_seq = 0
    for header, body in res.values():
        old = log_seq, accepted_seq
        new = header['log_seq'], header['accepted_seq']

        if new > old:
            blob = body
            log_seq = header['log_seq']
            commit_id = header['commit_id']
            accepted_seq = header['accepted_seq']

    if 0 == log_seq:
        return 'BLOB_NOT_FOUND', None, None

    # paxos ACCEPT phase - write a blob to bring all nodes in sync
    res = await rpc('accept', [proposal_seq, log_seq, commit_id], blob)
    if quorum > len(res):
        return 'NO_ACCEPT_QUORUM', None, None

    headers = {json.dumps(h, sort_keys=True) for h, _ in res.values()}
    if 1 == len(headers):
        log_seq = json.loads(headers.pop())['log_seq']
        return 'OK', [proposal_seq, log_seq + 1], None

    return 'ACCEPT_FAILED', None, None


class G:
    log_id = None
    logdir = None


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert, port = sys.argv[1], int(sys.argv[2])

    SSL = ssl.create_default_context(
        cafile=cert,
        purpose=ssl.Purpose.CLIENT_AUTH)
    SSL.load_cert_chain(cert, cert)
    SSL.verify_mode = ssl.CERT_REQUIRED

    # Extract UUID from the subject. This would be the log_id for this stream
    sub = SSL.get_ca_certs()[0]['subject'][0][0][1]
    guid = uuid.UUID(re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', sub)[0])

    G.log_id = str(guid)
    G.logdir = os.path.join('commitlog', G.log_id)
    os.makedirs(G.logdir, exist_ok=True)

    data_dirs = listdir(G.logdir)

    # Cleanup
    for f in os.listdir(G.logdir):
        if 'promised' == f:
            continue

        path = os.path.join(G.logdir, str(f))

        if f.isdigit():
            if int(f) < data_dirs[0] - 10:
                shutil.rmtree(path)
        else:
            if os.path.isfile(path):
                os.remove(path)
            else:
                shutil.rmtree(path)

    srv = await asyncio.start_server(server, None, port, ssl=SSL)
    async with srv:
        return await srv.serve_forever()


if '__main__' == __name__:
    asyncio.run(main())
