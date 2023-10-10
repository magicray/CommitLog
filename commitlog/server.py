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
import commitlog.http


def path_join(*path):
    return os.path.join(*[str(p) for p in path])


def seq2path(log_seq):
    return path_join(G.logdir, log_seq//100000, log_seq//1000, log_seq)


def sorted_dir(dirname):
    files = [int(f) for f in os.listdir(dirname) if f.isdigit()]
    return sorted(files, reverse=True)


async def fetch(log_seq, what):
    path = seq2path(int(log_seq))
    if os.path.isfile(path):
        with open(path, 'rb') as fd:
            return fd.read() if 'body' == what else json.loads(fd.readline())


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
async def paxos_promise(proposal_seq):
    proposal_seq = int(proposal_seq)

    with open(G.promise_filepath) as fd:
        promised_seq = json.load(fd)['promised_seq']

    # proposal_seq has to be strictly bigger than whatever seen so far
    if proposal_seq > promised_seq:
        dump(G.promise_filepath, dict(promised_seq=proposal_seq))

        # Traverse the three level directory hierarchy,
        # picking the highest numbered dir/file at each level
        for x in sorted_dir(G.logdir):
            for y in sorted_dir(path_join(G.logdir, x)):
                for f in sorted_dir(path_join(G.logdir, x, y)):
                    with open(seq2path(f), 'rb') as fd:
                        return fd.read()

        hdr = dict(log_seq=0, accepted_seq=0)
        return json.dumps(hdr, sort_keys=True).encode() + b'\n'


# ACCEPT - Client has sent the most recent value from the promise phase.
# Stale leaders blocked. Only the most recent can reach this stage.
async def paxos_accept(proposal_seq, log_seq, blob):
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

        hdr = dict(accepted_seq=proposal_seq, log_id=G.log_id, log_seq=log_seq,
                   length=len(blob), sha1=hashlib.sha1(blob).hexdigest())

        if blob:
            dump(seq2path(log_seq), hdr, b'\n', blob)
            hdr.pop('accepted_seq')
            return hdr


# Used for leader election - Ideally, this should be part of the client code.
# However, since leader election is done infrequently, its more maintainable to
# keep this code here and call it over RPC. This makes client very lightweight.
async def paxos_client(servers):
    servers = [s.split(':') for s in servers.split(',')]
    servers = [(ip, int(port)) for ip, port in servers]

    rpc = commitlog.http.Client(sys.argv[1], servers)
    quorum = int(len(servers)/2) + 1
    proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

    # Paxos PROMISE phase - block stale leaders from writing
    res = await rpc.cluster(f'/promise/proposal_seq/{proposal_seq}')
    if quorum > len(res):
        return

    hdrs = set(res.values())
    if 1 == len(hdrs):
        header = hdrs.pop().split(b'\n', maxsplit=1)[0]
        return [proposal_seq, json.loads(header)['log_seq']]

    # CRUX of the paxos protocol - Find the most recent log_seq with most
    # recent accepted_seq. Only this value should be proposed
    log_seq = accepted_seq = 0
    for val in res.values():
        header, body = val.split(b'\n', maxsplit=1)
        header = json.loads(header)

        old = log_seq, accepted_seq
        new = header['log_seq'], header['accepted_seq']

        if new > old:
            blob = body
            log_seq = header['log_seq']
            accepted_seq = header['accepted_seq']

    if 0 == log_seq or not blob:
        return

    # Paxos ACCEPT phase - re-write the last blob to bring all nodes in sync
    url = f'/commit/proposal_seq/{proposal_seq}/log_seq/{log_seq}'
    vlist = list((await rpc.cluster(url, blob)).values())

    if len(vlist) >= quorum and all([vlist[0] == v for v in vlist]):
        return [proposal_seq, json.loads(vlist[0])['log_seq']]


async def tail(log_seq, servers):
    seq = int(log_seq)

    servers = [s.split(':') for s in servers.split(',')]
    servers = [(ip, int(port)) for ip, port in servers]

    rpc = commitlog.http.Client(sys.argv[1], servers)
    quorum = int(len(servers)/2) + 1

    res = await rpc.cluster(f'/fetch/log_seq/{seq}/what/header')
    if quorum > len(res):
        return

    hdrs = list()
    for k, v in res.items():
        # accepted seq, header, server
        hdrs.append((v.pop('accepted_seq'), v, k))

    hdrs = sorted(hdrs, reverse=True)
    if not all([hdrs[0][1] == h[1] for h in hdrs[:quorum]]):
        return

    body = None
    path = seq2path(seq)
    if os.path.isfile(path):
        with open(path, 'rb') as fd:
            header = fd.readline()
            hdr = json.loads(header)
            hdr.pop('accepted_seq')
            if hdrs[0][1] == hdr:
                body = fd.read()

    if body:
        assert (hdr['length'] == len(body))
        return json.dumps(hdr, sort_keys=True).encode() + b'\n' + body

    url = f'/blob/log_seq/{seq}/what/body'
    result = await rpc.server(hdrs[0][2], url)
    if result:
        header, body = result.split(b'\n', max_split=1)
        hdr = json.loads(header)
        assert (hdr['length'] == len(body))

        hdr.pop('accepted_seq')
        assert (hdrs[0][1] == hdr)

        dump(path, hdr, b'\n', body)
        return json.dumps(hdr, sort_keys=True).encode() + b'\n' + body


class G:
    log_id = None
    logdir = None


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert, port = sys.argv[1], int(sys.argv[2])

    ctx = commitlog.http.Certificate.context(cert, ssl.Purpose.CLIENT_AUTH)
    sub = commitlog.http.Certificate.subject(ctx)

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

    server = commitlog.http.Server(dict(
        init=paxos_client, promise=paxos_promise, commit=paxos_accept,
        fetch=fetch, tail=tail))

    await server.run(port, cert)


if '__main__' == __name__:
    asyncio.run(main())
