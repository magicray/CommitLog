import os
import re
import sys
import ssl
import uuid
import random
import asyncio
import logging
import commitlog.http
from logging import critical as log


def max_seq(logdir):
    # Traverse the three level directory hierarchy picking the highest
    # numbered dir/file at each level
    l1_dirs = [int(f) for f in os.listdir(logdir) if f.isdigit()]
    for l1 in sorted(l1_dirs, reverse=True):
        l2_dirname = os.path.join(logdir, str(l1))
        l2_dirs = [int(f) for f in os.listdir(l2_dirname) if f.isdigit()]
        for l2 in sorted(l2_dirs, reverse=True):
            l3_dirname = os.path.join(l2_dirname, str(l2))
            files = [int(f) for f in os.listdir(l3_dirname) if f.isdigit()]
            for f in sorted(files, reverse=True):
                return f

    return 0


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert = sys.argv[1]

    SSL = ssl.create_default_context(
        cafile=cert,
        purpose=ssl.Purpose.CLIENT_AUTH)
    SSL.load_cert_chain(cert, cert)
    SSL.verify_mode = ssl.CERT_REQUIRED

    logdir = os.path.join('commitlog', str(uuid.UUID(
        re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}',
                  SSL.get_ca_certs()[0]['subject'][0][0][1])[0])))
    os.makedirs(logdir, exist_ok=True)

    servers = [argv.split(':') for argv in sys.argv[2:]]
    servers = [(ip, int(port)) for ip, port in servers]

    seq = max_seq(logdir) + 1
    client = commitlog.http.Client(cert, servers)

    server = random.choice(servers)
    while True:
        url = '/tail/log_seq/{}/servers/{}'.format(seq, ','.join(sys.argv[2:]))
        res = await client.server(server, url)
        if not res:
            await asyncio.sleep(5)
            server = random.choice(servers)
            continue

        l1, l2 = seq//(100000), seq//1000
        logfile = os.path.join(logdir, str(l1), str(l2), str(seq))

        tmpfile = os.path.join(logdir, str(uuid.uuid4()) + '.tmp')
        with open(tmpfile, 'wb') as fd:
            fd.write(res)
        os.makedirs(os.path.dirname(logfile), exist_ok=True)
        os.replace(tmpfile, logfile)

        with open(logfile) as fd:
            log(fd.readline().strip())

        seq += 1


if '__main__' == __name__:
    asyncio.run(main())
