import sys
import json
import time
import asyncio
import logging
import commitlog.http
from logging import critical as log


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert = sys.argv[1]
    servers = sys.argv[2:]

    srvs = [argv.split(':') for argv in servers]
    srvs = [(ip, int(port)) for ip, port in srvs]

    client = commitlog.http.Client(cert, srvs)
    quorum = int(len(srvs)/2) + 1

    try:
        srv_list = ','.join(servers)
        res = await client.cluster(f'init/servers/{srv_list}')
        res = sorted([json.loads(r) for r in res.values()])

        if not res:
            log('could not get proposal_seq')
            exit(1)

        proposal_seq, log_seq = res[-1]

        while True:
            blob = sys.stdin.buffer.read(1024*1024)
            if not blob:
                exit(0)

            ts = time.time()
            log_seq += 1
            url = f'commit/proposal_seq/{proposal_seq}/log_seq/{log_seq}'
            res = await client.cluster(url, blob)
            vset = set(res.values())
            if quorum > len(res) or 1 != len(vset):
                log('commit failed')
                exit(1)

            result = json.loads(vset.pop())
            result['msec'] = int((time.time() - ts) * 1000)
            log(result)
    except Exception as e:
        log(e)
        exit(1)


if '__main__' == __name__:
    asyncio.run(main())
