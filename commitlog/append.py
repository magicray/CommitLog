import sys
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
        url = '/init/servers/{}'.format(','.join(servers))
        vlist = sorted((await client.cluster(url)).values())
        if not vlist:
            log('could not get proposal_seq')
            exit(1)

        proposal_seq, log_seq = vlist[-1]

        while True:
            blob = sys.stdin.buffer.read(1024*1024)
            if not blob:
                exit(0)

            ts = time.time()
            log_seq += 1

            url = f'/commit/proposal_seq/{proposal_seq}/log_seq/{log_seq}'
            vlist = list((await client.cluster(url, blob)).values())
            if quorum > len(vlist) or not all([vlist[0] == v for v in vlist]):
                log('commit failed')
                exit(1)

            vlist[0]['msec'] = int((time.time() - ts) * 1000)
            log(vlist[0])
    except Exception as e:
        log(e)
        exit(1)


if '__main__' == __name__:
    asyncio.run(main())
