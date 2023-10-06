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
import commitlog.rpc


def path_join(*path):
    return os.path.join(*[str(p) for p in path])


def seq2path(log_seq):
    return path_join(G.logdir, log_seq//100000, log_seq//1000, log_seq)


def sorted_dir(dirname):
    files = [int(f) for f in os.listdir(dirname) if f.isdigit()]
    return sorted(files, reverse=True)


def latest_logseq():
    # Traverse the three level directory hierarchy,
    # picking the highest numbered dir/file at each level
    for x in sorted_dir(G.logdir):
        for y in sorted_dir(path_join(G.logdir, x)):
            for f in sorted_dir(path_join(G.logdir, x, y)):
                return f

    return 0


@commitlog.rpc.async_generator
async def logseq_server(header, body):
    return 'OK', latest_logseq(), None


@commitlog.rpc.async_generator
async def header_server(header, body):
    path = seq2path(header)
    if not os.path.isfile(path):
        return 'NOTFOUND', None, None

    with open(path, 'rb') as fd:
        return 'OK', json.loads(fd.readline()), None


@commitlog.rpc.async_generator
async def body_server(header, body):
    path = seq2path(header)
    if not os.path.isfile(path):
        return 'NOTFOUND', None, None

    with open(path, 'rb') as fd:
        return 'OK', json.loads(fd.readline()), fd.read()


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


# PROMISE - Block stale leaders and return the most recent accepted value.
# Client will propose the most recent across servers in the accept phase
@commitlog.rpc.async_generator
async def paxos_promise(header, body):
    proposal_seq = header

    with open(G.promise_filepath) as fd:
        promised_seq = json.load(fd)['promised_seq']

    # proposal_seq has to be strictly bigger than whatever seen so far
    if proposal_seq > promised_seq:
        dump(G.promise_filepath, dict(promised_seq=proposal_seq))

        log_seq = latest_logseq()
        if 0 == log_seq:
            return 'OK', dict(log_seq=0, accepted_seq=0), None

        with open(seq2path(log_seq), 'rb') as fd:
            return 'OK', json.loads(fd.readline()), fd.read()

    return 'STALE_PROPOSAL_SEQ', None, None


# ACCEPT - Client has sent the most recent value from the promise phase.
# Stale leaders blocked. Only the most recent can reach this stage.
@commitlog.rpc.async_generator
async def paxos_accept(header, body):
    proposal_seq, log_seq, commit_id = header

    if not body:
        return 'EMPTY_BLOB', None, None

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

        header = dict(accepted_seq=proposal_seq, log_id=G.log_id,
                      log_seq=log_seq, commit_id=commit_id,
                      length=len(body), sha1=hashlib.sha1(body).hexdigest())

        dump(seq2path(log_seq), header, b'\n', body)

        header.pop('accepted_seq')
        return 'OK', header, None

    return 'STALE_PROPOSAL_SEQ', None, None


# Used for leader election - Ideally, this should be part of the client code.
# However, since leader election is done infrequently, its more maintainable to
# keep this code here and call it over RPC. This makes client very lightweight.
@commitlog.rpc.async_generator
async def paxos_client(header, body):
    cert, servers = sys.argv[1], header

    rpc = commitlog.rpc.Client(cert, servers)
    quorum = int(len(servers)/2) + 1
    proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

    # paxos PROMISE phase - block stale leaders from writing
    res = await rpc('promise', proposal_seq)
    if quorum > len(res):
        return 'NO_PROMISE_QUORUM', None, None

    hdrs = {json.dumps(h, sort_keys=True) for h, _ in res.values()}
    if 1 == len(hdrs):
        return 'OK', [proposal_seq, json.loads(hdrs.pop())['log_seq']], None

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

    if 0 == log_seq or not blob:
        return 'BLOB_NOT_FOUND', None, None

    # paxos ACCEPT phase - re-write the last blob to bring all nodes in sync
    res = await rpc('accept', [proposal_seq, log_seq, commit_id], blob)
    hdrs = {json.dumps(h, sort_keys=True) for h, _ in res.values()}

    if quorum > len(res) or 1 != len(hdrs):
        return 'NO_ACCEPT_QUORUM', None, None

    return 'OK', [proposal_seq, json.loads(hdrs.pop())['log_seq']], None


async def tail_server(header, data):
    cert, (seq, servers) = sys.argv[1], header

    rpc = commitlog.rpc.Client(cert, servers)
    quorum = int(len(servers)/2) + 1

    while True:
        res = await rpc('header', seq)
        if quorum > len(res):
            yield 'WAIT', None, None
            await asyncio.sleep(1)
            continue

        hdrs = list()
        for k, v in res.items():
            hdrs.append((
                v[0].pop('accepted_seq'),          # accepted seq
                json.dumps(v[0], sort_keys=True),  # record metadata
                k))                                # server

        hdrs = sorted(hdrs, reverse=True)
        if not all([hdrs[0][1] == h[1] for h in hdrs[:quorum]]):
            yield 'WAIT', None, None
            await asyncio.sleep(1)
            continue

        path = seq2path(seq)
        body = None
        if os.path.isfile(path):
            with open(path, 'rb') as fd:
                hdr = json.loads(fd.readline())
                hdr.pop('accepted_seq')
                if hdrs[0][1] == json.dumps(hdr, sort_keys=True):
                    body = fd.read()

        if body:
            assert (hdr['length'] == len(body))
            yield 'OK', hdr, body
            seq = seq + 1
        else:
            result = await rpc.server(hdrs[0][2], 'body', seq)
            if result and 'OK' == result[0]:
                assert (result[1]['length'] == len(result[2]))

                tmp = result[1].copy()
                tmp.pop('accepted_seq')
                assert (hdrs[0][1] == json.dumps(tmp, sort_keys=True))

                dump(path, result[1], b'\n', result[2])
            else:
                yield 'WAIT', None, None
                await asyncio.sleep(1)


class G:
    log_id = None
    logdir = None


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert, port = sys.argv[1], int(sys.argv[2])

    ctx = commitlog.rpc.Certificate.context(cert, ssl.Purpose.CLIENT_AUTH)
    sub = commitlog.rpc.Certificate.subject(ctx)

    # Extract UUID from the subject. This would be the log_id for this stream
    guid = uuid.UUID(re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', sub)[0])

    G.log_id = str(guid)
    G.logdir = os.path.join('commitlog', G.log_id)
    G.promise_filepath = os.path.join(G.logdir, 'promised')

    if not os.path.isfile(G.promise_filepath):
        dump(G.promise_filepath, dict(promised_seq=0))

    # Cleanup
    data_dirs = sorted_dir(G.logdir)
    for f in os.listdir(G.logdir):
        path = os.path.join(G.logdir, str(f))

        if 'promised' == f:
            continue

        if f.isdigit() and int(f) > data_dirs[0]-10 and os.path.isdir(path):
            continue

        os.remove(path) if os.path.isfile(path) else shutil.rmtree(path)

    await commitlog.rpc.server(port, cert, dict(
        promise=paxos_promise, accept=paxos_accept, grant=paxos_client,
        logseq=logseq_server, header=header_server, body=body_server,
        tail=tail_server))


if '__main__' == __name__:
    asyncio.run(main())
