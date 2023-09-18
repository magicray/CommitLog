import sys
import json
import time
import asyncio
import logging
from logging import critical as log

import commitlog.client


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert = sys.argv[1]

    # Tail
    if sys.argv[-1].isdigit():
        servers = [argv.split(':') for argv in sys.argv[2:-1]]
        servers = [(ip, int(port)) for ip, port in servers]

        client = commitlog.client.Client(cert, servers)
        log_seq = int(sys.argv[-1])

        while True:
            async for meta, data in client.tail(log_seq):
                assert len(data) == meta['length']
                log(json.dumps(meta, indent=4, sort_keys=True))
                log_seq = meta['log_seq'] + 1

            await asyncio.sleep(1)

    # Append
    else:
        servers = [argv.split(':') for argv in sys.argv[2:]]
        servers = [(ip, int(port)) for ip, port in servers]

        client = commitlog.client.Client(cert, servers)

        try:
            result = await client.commit()
            log(json.dumps(result, indent=4, sort_keys=True))

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
