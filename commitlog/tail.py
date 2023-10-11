import os
import re
import sys
import ssl
import json
import uuid
import asyncio
import logging
import commitlog
import commitlog.http
from logging import critical as log


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert = sys.argv[1]

    SSL = ssl.create_default_context(
        cafile=cert,
        purpose=ssl.Purpose.CLIENT_AUTH)
    SSL.load_cert_chain(cert, cert)

    logdir = os.path.join('commitlog', str(uuid.UUID(
        re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}',
                  SSL.get_ca_certs()[0]['subject'][0][0][1])[0])))

    os.makedirs(logdir, exist_ok=True)
    seq = commitlog.max_seq(logdir) + 1

    servers = [argv.split(':') for argv in sys.argv[2:]]
    servers = [(ip, int(port)) for ip, port in servers]
    client = commitlog.http.Client(cert, servers)
    quorum = int(len(servers)/2) + 1

    while True:
        res = await client.cluster(f'/fetch/log_seq/{seq}/what/header')
        if quorum > len(res):
            await asyncio.sleep(1)
            continue

        hdrs = list()
        for k, v in res.items():
            # accepted seq, header, server
            hdrs.append((v.pop('accepted_seq'), v, k))

        hdrs = sorted(hdrs, reverse=True)
        if not all([hdrs[0][1] == h[1] for h in hdrs[:quorum]]):
            await asyncio.sleep(10)
            continue

        url = f'/fetch/log_seq/{seq}/what/body'
        result = await client.server(hdrs[0][2], url)
        if not result:
            await asyncio.sleep(1)
            continue

        header, body = result.split(b'\n', maxsplit=1)
        hdr = json.loads(header)

        hdr.pop('accepted_seq')
        assert (hdr['length'] == len(body))
        assert (hdrs[0][1] == hdr)

        path = commitlog.seq2path(logdir, seq)
        commitlog.dump(logdir, path, hdr, b'\n', body)

        with open(path) as fd:
            log(fd.readline().strip())

        seq += 1


if '__main__' == __name__:
    asyncio.run(main())
