import os
import ssl
import uuid
import asyncio
import logging
import argparse
import commitlog
import commitlog.server
from logging import critical as log


async def cmd_tail(G):
    G.client = commitlog.Client(G.cacert, G.cert, G.servers)
    ctx = ssl.create_default_context(cafile=G.cert)
    log_id = str(uuid.UUID(ctx.get_ca_certs()[0]['subject'][0][0][1]))

    os.makedirs(commitlog.server.path_join('commitlog', log_id), exist_ok=True)

    seq = commitlog.server.get_max_seq(log_id) + 1
    delay = 1
    max_seq = 0

    while True:
        try:
            if seq >= max_seq:
                max_seq = await G.client.max_seq()
                if seq >= max_seq:
                    raise Exception('SEQ_OUT_OF_RANGE')

            hdr, octets = await G.client.tail(seq)
            path = commitlog.server.seq2path(log_id, seq)
            commitlog.server.dump(path, hdr, b'\n', octets)
            log(hdr)

            seq += 1
            delay = 1
        except Exception as e:
            log(f'wait({delay}) seq({seq}) max({max_seq}) exception({e})')
            await asyncio.sleep(delay)
            delay = min(60, 2*delay)


if '__main__' == __name__:
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    G = argparse.ArgumentParser()
    G.add_argument('--cert', help='certificate path')
    G.add_argument('--cacert', help='ca certificate path')
    G.add_argument('--servers', help='comma separated list of server ip:port')
    G = G.parse_args()

    asyncio.run(cmd_tail(G))
