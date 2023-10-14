import os
import sys
import time
import asyncio
import logging
import argparse
import commitlog
from logging import critical as log


async def append():
    client = commitlog.Client(G.cert, G.servers)

    if await client.init() is None:
        log('init failed')
        exit(1)

    while True:
        octets = sys.stdin.buffer.read(1024*1024)
        if not octets:
            exit(0)

        ts = time.time()

        result = await client.write(octets)
        if not result:
            log('commit failed')
            exit(1)

        result['msec'] = int((time.time() - ts) * 1000)
        log(result)


async def tail():
    seq = commitlog.max_seq(logdir) + 1
    client = commitlog.Client(G.cert, G.servers)

    while True:
        result = await client.read(seq)
        if not result:
            await asyncio.sleep(1)
            continue

        hdr, octets = result

        path = commitlog.seq2path(logdir, seq)
        commitlog.dump(path, hdr, b'\n', octets)

        with open(path) as fd:
            log(fd.readline().strip())

        seq += 1


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--cmd', help='command - tail/append')
    G.add_argument('--cert', help='Self signed certificate file path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G = G.parse_args()

    logdir = os.path.join('commitlog', commitlog.cert_uuid(G.cert))
    os.makedirs(logdir, exist_ok=True)

    asyncio.run(globals()[G.cmd]())
