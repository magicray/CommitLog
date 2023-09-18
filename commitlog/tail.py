import sys
import json
import asyncio
import logging
from logging import critical as log

import commitlog.client


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert, seq = sys.argv[1], int(sys.argv[-1])

    servers = [argv.split(':') for argv in sys.argv[2:-1]]
    servers = [(ip, int(port)) for ip, port in servers]

    client = commitlog.client.Client(cert, servers)

    while True:
        async for meta, data in client.tail(seq):
            assert len(data) == meta['length']

            log(json.dumps(meta, indent=4, sort_keys=True))

            seq = meta['log_seq'] + 1

        await asyncio.sleep(1)


if '__main__' == __name__:
    asyncio.run(main())
