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
import commitlog
import commitlog.http


def seq2path(log_seq):
    return commitlog.seq2path(G.logdir, log_seq)


def dump(path, *objects):
    commitlog.dump(G.logdir, path, *objects)
    os.sync()


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

        max_seq = commitlog.max_seq(G.logdir)
        if max_seq > 0:
            with open(seq2path(max_seq), 'rb') as fd:
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
        return [proposal_seq, vlist[0]['log_seq']]


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
    data_dirs = commitlog.sorted_dir(G.logdir)
    for f in os.listdir(G.logdir):
        path = os.path.join(G.logdir, str(f))

        if 'promised' == f:
            continue

        if f.isdigit() and int(f) > data_dirs[0]-10 and os.path.isdir(path):
            continue

        os.remove(path) if os.path.isfile(path) else shutil.rmtree(path)

    server = commitlog.http.Server(dict(
        fetch=fetch, init=paxos_client,
        promise=paxos_promise, commit=paxos_accept))

    await server.run(port, cert)


if '__main__' == __name__:
    asyncio.run(main())
