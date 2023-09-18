import sys
import json
import time
import asyncio
import logging
from logging import critical as log

import commitlog.client


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    client = commitlog.client.Client(sys.argv[1])
    log_id = sys.argv[2]

    # Append
    if 3 == len(sys.argv):
        try:
            result = await client.commit(log_id)
            log(json.dumps(result, indent=4, sort_keys=True))

            while True:
                blob = sys.stdin.buffer.read(1024*1024)
                if not blob:
                    exit(0)

                ts = time.time()
                result = await client.commit(log_id, blob)

                if not result:
                    exit(1)

                result['msec'] = int((time.time() - ts) * 1000)
                log(json.dumps(result, indent=4, sort_keys=True))
        except Exception as e:
            log(e)
            exit(1)

    # Tail
    elif 4 == len(sys.argv) and sys.argv[3].isdigit():
        log_seq = int(sys.argv[3])

        while True:
            async for meta, data in client.tail(log_id, log_seq):
                assert len(data) == meta['length']
                log(json.dumps(meta, indent=4, sort_keys=True))
                log_seq = meta['log_seq'] + 1

            await asyncio.sleep(1)


if '__main__' == __name__:
    asyncio.run(main())
