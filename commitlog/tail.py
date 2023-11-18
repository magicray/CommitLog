import os
import ssl
import uuid
import json
import asyncio
import logging
import argparse
import commitlog
import commitlog.rpc
from logging import critical as log


def path_join(*path):
    return os.path.join(*[str(p) for p in path])


def seq2path(log_id, log_seq):
    x, y = log_seq//1000000, log_seq//1000
    return path_join('commitlog', log_id, x, y, log_seq)


def get_max_seq(log_id):
    def reverse_sorted_dir(dirname):
        files = [int(f) for f in os.listdir(dirname) if f.isdigit()]
        return sorted(files, reverse=True)

    # Traverse the three level directory hierarchy,
    # picking the highest numbered dir/file at each level
    logdir = path_join('commitlog', log_id)
    for x in reverse_sorted_dir(logdir):
        for y in reverse_sorted_dir(path_join(logdir, x)):
            for f in reverse_sorted_dir(path_join(logdir, x, y)):
                return f

    return 0


def dump(path, *objects):
    path = os.path.abspath(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp = path + '.' + str(uuid.uuid4()) + '.tmp'
    with open(tmp, 'wb') as fd:
        for obj in objects:
            if type(obj) is not bytes:
                obj = json.dumps(obj, sort_keys=True).encode()

            fd.write(obj)

    os.replace(tmp, path)


async def cmd_tail(G):
    G.client = commitlog.Client(G.cacert, G.cert, G.servers)
    ctx = ssl.create_default_context(cafile=G.cert)
    log_id = str(uuid.UUID(ctx.get_ca_certs()[0]['subject'][0][0][1]))

    os.makedirs(path_join('commitlog', log_id), exist_ok=True)

    seq = get_max_seq(log_id) + 1
    delay = 1
    max_seq = 0

    while True:
        try:
            if seq >= max_seq:
                max_seq = await G.client.max_seq()
                if seq >= max_seq:
                    raise Exception('SEQ_OUT_OF_RANGE')

            hdr, octets = await G.client.tail(seq)
            dump(seq2path(log_id, seq), hdr, b'\n', octets)
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
