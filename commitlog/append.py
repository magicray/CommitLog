import sys
import asyncio
import logging
import argparse
import commitlog
import commitlog.rpc
from logging import critical as log


async def cmd_append(G):
    try:
        client = commitlog.Client(G.cacert, G.cert, G.servers)
        await client.reset()
        log(await client.append(sys.stdin.buffer.read()))
    except Exception as e:
        log(e)
        exit(1)


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--cacert', help='ca certificate path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G = G.parse_args()

    asyncio.run(cmd_append(G))
