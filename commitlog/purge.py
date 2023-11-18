import asyncio
import logging
import argparse
import commitlog
from logging import critical as log


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--cacert', help='ca certificate path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G.add_argument('--purge', type=int, help='purge before this seq number')
    G = G.parse_args()

    client = commitlog.Client(G.cacert, G.cert, G.servers)
    log(asyncio.run(client.purge(G.purge)))
