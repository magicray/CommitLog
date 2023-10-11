import os
import re
import sys
import ssl
import json
import time
import uuid
import asyncio
import logging
import commitlog
from logging import critical as log


async def append():
    client = commitlog.Client(cert, servers)

    if not await client.init():
        log('init failed')
        exit(1)

    while True:
        blob = sys.stdin.buffer.read(1024*1024)
        if not blob:
            exit(0)

        ts = time.time()

        result = await client.commit(blob)
        if not result:
            log('commit failed')
            exit(1)

        result['msec'] = int((time.time() - ts) * 1000)
        log(result)


async def tail():
    quorum = int(len(servers)/2) + 1
    client = commitlog.HTTPClient(cert, servers)

    seq = commitlog.max_seq(logdir) + 1

    while True:
        res = await client.cluster(f'/fetch/log_seq/{seq}/what/header')
        if quorum > len(res):
            await asyncio.sleep(10)
            continue

        hdrs = list()
        for k, v in res.items():
            # accepted seq, header, server
            hdrs.append((v.pop('accepted_seq'), v, k))

        hdrs = sorted(hdrs, reverse=True)
        if not all([hdrs[0][1] == h[1] for h in hdrs[:quorum]]):
            await asyncio.sleep(1)
            continue

        url = f'/fetch/log_seq/{seq}/what/body'
        result = await client.server(hdrs[0][2], url)
        if not result:
            await asyncio.sleep(1)
            continue

        header, body = result.split(b'\n', maxsplit=1)
        hdr = json.loads(header)

        hdr.pop('accepted_seq')
        assert (hdr['length'] == len(body))
        assert (hdrs[0][1] == hdr)

        path = commitlog.seq2path(logdir, seq)
        commitlog.dump(path, hdr, b'\n', body)

        with open(path) as fd:
            log(fd.readline().strip())

        seq += 1


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cmd, cert, servers = sys.argv[1], sys.argv[2], sys.argv[3:]
    servers = [(ip, int(port)) for ip, port in [s.split(':') for s in servers]]

    ctx = commitlog.Certificate.context(cert, ssl.Purpose.CLIENT_AUTH)
    logdir = os.path.join('commitlog', str(uuid.UUID(
        re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}',
                  commitlog.Certificate.subject(ctx))[0])))

    os.makedirs(logdir, exist_ok=True)
    asyncio.run(globals()[cmd]())
