import os
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


async def rpc_max_seq(ctx):
    log_id = ctx['subject']

    return json.dumps(get_max_seq(log_id)).encode()


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


async def read(ctx, log_seq, length=-1):
    log_id = ctx['subject']
    length = int(length)
    log_seq = int(log_seq)
    path = seq2path(log_id, log_seq)

    if os.path.isfile(path):
        with open(path, 'rb') as fd:
            return fd.read(length)

    return json.dumps(dict(accepted_seq=0)).encode()


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

    if proposal_seq-10 > int(time.strftime('%Y%m%d%H%M%S')) > proposal_seq+10:
        raise Exception('CLOCKS_OUT_OF_SYNC')

    os.makedirs(logdir, exist_ok=True)
    lockfd = os.open(logdir, os.O_RDONLY)
    try:
        fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        if proposal_seq <= get_promised_seq(logdir):
            raise Exception(f'INVALID_PROMISE_SEQ {proposal_seq}')

        # Record new proposal_seq as it is bigger than the current value.
        # Any future writes with a smaller seq would be rejected.
        put_promised_seq(logdir, proposal_seq)

        # Paxos PROMISE response - return latest log record
        max_seq = get_max_seq(log_id)
        if max_seq > 0:
            with open(seq2path(log_id, max_seq), 'rb') as fd:
                return fd.read()

        return json.dumps(dict(log_seq=0, accepted_seq=0)).encode() + b'\n'
    finally:
        os.sync()
        os.close(lockfd)


# ACCEPT - Client has sent the most recent value from the promise phase.
# Stale leaders blocked. Only the most recent can reach this stage.
async def paxos_accept(ctx, proposal_seq, log_seq, checksum, octets):
    log_id = ctx['subject']
    logdir = path_join('commitlog', log_id)
    log_seq = int(log_seq)
    proposal_seq = int(proposal_seq)

    if not octets or type(octets) is not bytes:
        raise Exception('INVALID_OCTETS')

    if log_seq < 1:
        raise Exception('INVALID_LOGSEQ')

    os.makedirs(logdir, exist_ok=True)
    lockfd = os.open(logdir, os.O_RDONLY)
    try:
        fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        promised_seq = get_promised_seq(logdir)
        if proposal_seq < promised_seq:
            raise Exception(f'INVALID_ACCEPT_SEQ {proposal_seq}')

        # Record new proposal_seq as it is bigger than the current value.
        # Any future writes with a smaller seq would be rejected.
        if proposal_seq > promised_seq:
            put_promised_seq(logdir, proposal_seq)

        # Paxos ACCEPT response - Save octets and return success
        hdr = dict(accepted_seq=proposal_seq, log_seq=log_seq, log_id=log_id,
                   checksum=checksum, length=len(octets))

        path = seq2path(log_id, log_seq)
        dump(path, hdr, b'\n', octets)

        with open(path) as fd:
            return fd.readline()
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
                    return json.dumps(count).encode()

                os.remove(path_join(logdir, x, y, f))
                count += 1

            shutil.rmtree(path_join(logdir, x, y))
        shutil.rmtree(path_join(logdir, x))


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--port', help='port number for server')
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--cacert', help='ca certificate path')
    G = G.parse_args()

    asyncio.run(commitlog.rpc.Server().run(G.cacert, G.cert, G.port, dict(
        read=read, purge=purge, max_seq=rpc_max_seq,
        promise=paxos_promise, commit=paxos_accept)))
