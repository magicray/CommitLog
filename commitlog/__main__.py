import os
import sys
import ssl
import time
import uuid
import json
import fcntl
import shutil
import asyncio
import logging
import argparse
import commitlog
import commitlog.rpc
from logging import critical as log


def path_join(*path):
    return os.path.join(*[str(p) for p in path])


def seq2path(log_id, log_seq):
    x, y = log_seq//1000000, log_seq//1000
    return path_join('commitlog', log_id, x, y, log_seq)


def get_max_seq(log_id):
    def reverse_sorted_dir(dirname):
        files = [int(f) for f in os.listdir(dirname) if f.isdigit()]
        return sorted(files, reverse=True)

    # Traverse the three level directory hierarchy,
    # picking the highest numbered dir/file at each level
    logdir = path_join('commitlog', log_id)
    for x in reverse_sorted_dir(logdir):
        for y in reverse_sorted_dir(path_join(logdir, x)):
            for f in reverse_sorted_dir(path_join(logdir, x, y)):
                return f

    return 0


def dump(path, *objects):
    path = os.path.abspath(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp = path + '.' + str(uuid.uuid4()) + '.tmp'
    with open(tmp, 'wb') as fd:
        for obj in objects:
            if type(obj) is not bytes:
                obj = json.dumps(obj, sort_keys=True).encode()

            fd.write(obj)

    os.replace(tmp, path)


async def read(ctx, log_seq, what):
    path = seq2path(ctx['subject'], int(log_seq))

    if os.path.isfile(path):
        with open(path, 'rb') as fd:
            return fd.read() if 'body' == what else json.loads(fd.readline())


def get_promised_seq(logdir):
    path = path_join(logdir, 'promised')

    if os.path.isfile(path):
        with open(path) as fd:
            return json.load(fd)['promised_seq']

    return 0


def put_promised_seq(logdir, seq):
    dump(path_join(logdir, 'promised'), dict(promised_seq=seq))


# PROMISE - Block stale leaders and return the most recent accepted value.
# Client will propose the most recent across servers in the accept phase
async def paxos_promise(ctx, proposal_seq):
    log_id = ctx['subject']
    logdir = path_join('commitlog', log_id)
    proposal_seq = int(proposal_seq)

    os.makedirs(logdir, exist_ok=True)

    lockfd = os.open(logdir, os.O_RDONLY)
    try:
        fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        # Record new proposal_seq as it is bigger than the current value.
        # Any future writes with a smaller seq would be rejected.
        if proposal_seq > get_promised_seq(logdir):
            put_promised_seq(logdir, proposal_seq)

            # Paxos PROMISE response - return latest log record
            max_seq = get_max_seq(log_id)
            if max_seq > 0:
                with open(seq2path(log_id, max_seq), 'rb') as fd:
                    return json.loads(fd.readline())

            return dict(log_seq=0, accepted_seq=0)
    finally:
        os.sync()
        os.close(lockfd)


# ACCEPT - Client has sent the most recent value from the promise phase.
# Stale leaders blocked. Only the most recent can reach this stage.
async def paxos_accept(ctx, proposal_seq, log_seq, commit_id, octets):
    log_id = ctx['subject']
    logdir = path_join('commitlog', log_id)
    log_seq = int(log_seq)
    proposal_seq = int(proposal_seq)

    if not octets or type(octets) is not bytes:
        raise Exception('INVALID_OCTETS')

    os.makedirs(logdir, exist_ok=True)

    lockfd = os.open(logdir, os.O_RDONLY)
    try:
        fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        promised_seq = get_promised_seq(logdir)

        # Record new proposal_seq as it is bigger than the current value.
        # Any future writes with a smaller seq would be rejected.
        if proposal_seq > promised_seq:
            put_promised_seq(logdir, proposal_seq)

        # Paxos ACCEPT response - Save octets and return success
        if proposal_seq >= promised_seq:
            hdr = dict(accepted_seq=proposal_seq, log_seq=log_seq,
                       log_id=log_id, commit_id=commit_id, length=len(octets))

            dump(seq2path(log_id, log_seq), hdr, b'\n', octets)

            return hdr
    finally:
        os.sync()
        os.close(lockfd)


async def purge(ctx, log_seq):
    log_id = ctx['subject']
    logdir = path_join('commitlog', log_id)
    log_seq = int(log_seq)

    def sorted_dir(dirname):
        return sorted([int(f) for f in os.listdir(dirname) if f.isdigit()])

    count = 0
    for x in sorted_dir(logdir):
        for y in sorted_dir(path_join(logdir, x)):
            for f in sorted_dir(path_join(logdir, x, y)):
                if f > log_seq:
                    return count

                os.remove(path_join(logdir, x, y, f))
                count += 1

            shutil.rmtree(path_join(logdir, x, y))
        shutil.rmtree(path_join(logdir, x))


async def cmd_append():
    ts = time.time()

    if not os.path.isfile(G.append):
        obj = await G.client.reset()
    else:
        with open(G.append) as fd:
            obj = json.load(fd)
        os.remove(G.append)
        G.client.init(obj['proposal_seq'], obj['log_seq'])

    if obj is None:
        log('reset failed')
        exit(1)

    result = await G.client.append(sys.stdin.buffer.read())
    if not result:
        log('commit failed')
        exit(1)

    obj.update(result)
    obj['proposal_seq'] = obj['accepted_seq']
    obj['msec'] = int((time.time() - ts) * 1000)
    dump(G.append, obj)
    log(obj)


async def cmd_backup(log_id):
    os.makedirs(path_join('commitlog', log_id), exist_ok=True)

    seq = get_max_seq(log_id) + 1
    delay = 1

    while True:
        result = await G.client.tail(seq)
        if not result:
            # exponential backoff
            await asyncio.sleep(delay)
            delay = min(32, 2*delay)
            continue

        hdr, octets = result

        path = seq2path(log_id, seq)
        dump(path, hdr, b'\n', octets)

        with open(path) as fd:
            log(fd.readline().strip())

        seq += 1
        delay = 1


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--port', help='port number for server')
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--cacert', help='ca certificate path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G.add_argument('--append', help='filename to store/read leader state')
    G.add_argument('--purge', type=int, help='purge before this seq number')
    G = G.parse_args()

    if G.servers:
        G.client = commitlog.Client(G.cacert, G.cert, G.servers)

    if G.port:
        asyncio.run(commitlog.rpc.Server().run(G.cacert, G.cert, G.port, dict(
            read=read, purge=purge,
            promise=paxos_promise, commit=paxos_accept)))
    elif G.append:
        asyncio.run(cmd_append())
    elif G.purge:
        log(asyncio.run(G.client.purge(G.purge)))
    else:
        ctx = ssl.create_default_context(cafile=G.cert)
        log_id = str(uuid.UUID(ctx.get_ca_certs()[0]['subject'][0][0][1]))

        asyncio.run(cmd_backup(log_id))
