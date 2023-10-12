import os
import re
import sys
import ssl
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
    seq = commitlog.max_seq(logdir) + 1
    client = commitlog.Client(cert, servers)

    async for hdr, blob in client.tail(seq):
        path = commitlog.seq2path(logdir, hdr['log_seq'])
        commitlog.dump(path, hdr, b'\n', blob)

        with open(path) as fd:
            log(fd.readline().strip())


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
