import os
import sys
import json
import time
import shutil
import hashlib
import asyncio
import logging
import argparse
import commitlog
from logging import critical as log


def seq2path(log_seq):
    return commitlog.seq2path(G.logdir, log_seq)


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
        commitlog.dump(G.promise_filepath, dict(promised_seq=proposal_seq))
        os.sync()

        max_seq = commitlog.max_seq(G.logdir)
        if max_seq > 0:
            with open(seq2path(max_seq), 'rb') as fd:
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
            commitlog.dump(G.promise_filepath, dict(promised_seq=proposal_seq))

        hdr = dict(accepted_seq=proposal_seq, log_id=G.log_id, log_seq=log_seq,
                   commit_id=commit_id, length=len(octets),
                   sha1=hashlib.sha1(octets).hexdigest())

        if octets:
            commitlog.dump(seq2path(log_seq), hdr, b'\n', octets)
            os.sync()

            return hdr


async def init():
    return dict(log_seq=await G.client.init())


async def echo(msg):
    return dict(msg=msg)


async def server():
    G.promise_filepath = os.path.join(G.logdir, 'promised')

    if not os.path.isfile(G.promise_filepath):
        commitlog.dump(G.promise_filepath, dict(promised_seq=0))

    # Cleanup
    data_dirs = commitlog.sorted_dir(G.logdir)
    for f in os.listdir(G.logdir):
        path = os.path.join(G.logdir, str(f))

        if 'promised' == f:
            continue

        if f.isdigit() and int(f) > data_dirs[0]-10 and os.path.isdir(path):
            continue

        os.remove(path) if os.path.isfile(path) else shutil.rmtree(path)

    server = commitlog.HTTPServer(dict(
        echo=echo, fetch=fetch, init=init,
        promise=paxos_promise, commit=paxos_accept))

    await server.run(G.port, G.cert)


async def append():
    if await G.client.init() is None:
        log('init failed')
        exit(1)

    while True:
        octets = sys.stdin.buffer.read(1024*1024)
        if not octets:
            exit(0)

        ts = time.time()

        result = await G.client.write(octets)
        if not result:
            log('commit failed')
            exit(1)

        result['msec'] = int((time.time() - ts) * 1000)
        log(result)


async def tail():
    seq = commitlog.max_seq(G.logdir) + 1

    while True:
        result = await G.client.read(seq)
        if not result:
            await asyncio.sleep(1)
            continue

        hdr, octets = result

        path = commitlog.seq2path(G.logdir, seq)
        commitlog.dump(path, hdr, b'\n', octets)

        with open(path) as fd:
            log(fd.readline().strip())

        seq += 1


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--cmd', help='command - tail/append')
    G.add_argument('--port', help='port number for server')
    G.add_argument('--cert', help='Self signed certificate file path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G = G.parse_args()

    G.log_id = str(commitlog.cert_uuid(G.cert))
    G.logdir = os.path.join('commitlog', G.log_id)
    os.makedirs(G.logdir, exist_ok=True)

    if G.servers:
        G.client = commitlog.Client(G.cert, G.servers)

    if G.port:
        asyncio.run(server())
    elif 'append' == G.cmd:
        asyncio.run(append())
    elif 'tail' == G.cmd:
        asyncio.run(tail())
