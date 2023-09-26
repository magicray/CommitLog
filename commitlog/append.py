import sys
import json
import time
import asyncio
import logging
import commitlog
from logging import critical as log


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert = sys.argv[1]

    servers = [argv.split(':') for argv in sys.argv[2:]]
    servers = [(ip, int(port)) for ip, port in servers]

    client = commitlog.Client(cert, servers)

    try:
        await client.lead()

        while True:
            blob = sys.stdin.buffer.read(1024*1024)
            if not blob:
                exit(0)

            ts = time.time()
            result = await client.commit(blob)

            if not result:
                exit(1)

            result['msec'] = int((time.time() - ts) * 1000)
            log(json.dumps(result, indent=4, sort_keys=True))
    except Exception as e:
        log(e)
        exit(1)


if '__main__' == __name__:
    asyncio.run(main())
