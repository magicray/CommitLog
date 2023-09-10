import os
import sys
import json
import time
import uuid
import hashlib
import asyncio
import logging
import traceback
from logging import critical as log


async def request_handler(reader, writer):
    HANDLERS = dict(promise=paxos_promise, accept=paxos_accept,
                    read=paxos_read)

    peer = writer.get_extra_info('socket').getpeername()

    while True:
        try:
            try:
                line = await reader.readline()
                if not line:
                    return writer.close()

                cmd, req_length, req_hdr = json.loads(line)
            except Exception:
                log('req{} disconnected or invalid header'.format(peer))
                return writer.close()

            if cmd not in HANDLERS:
                log('req{} {} invalid command'.format(peer, cmd))
                return writer.close()

            try:
                status, res_hdr, data = HANDLERS[cmd](
                    req_hdr,
                    await reader.readexactly(req_length))
            except Exception as e:
                traceback.print_exc()
                status, res_hdr, data = 'EXCEPTION', str(e), None

            res_length = len(data) if data else 0

            writer.write(json.dumps([status, res_length, res_hdr]).encode())
            writer.write(b'\n')
            if res_length > 0:
                writer.write(data)

            await writer.drain()
            log('{} {}:{} {} {} {} {}'.format(
                peer, cmd, status, req_length, req_hdr, res_length, res_hdr))
        except Exception as e:
            log('req{} {}'.format(peer, e))
            os._exit(0)


class RPC():
    def __init__(self, servers):
        self.conns = dict()

        for srv in servers:
            ip, port = srv.split(':')
            self.conns[(ip, int(port))] = None, None

    async def _rpc(self, server, cmd, meta=None, data=b''):
        try:
            if self.conns[server][0] is None or self.conns[server][1] is None:
                self.conns[server] = await asyncio.open_connection(
                    server[0], server[1])

            reader, writer = self.conns[server]

            if data and type(data) is not bytes:
                try:
                    data = json.dumps(data).encode()
                except Exception as e:
                    data = str(e).encode()

            length = len(data) if data else 0

            writer.write(json.dumps([cmd, length, meta]).encode())
            writer.write(b'\n')
            if length > 0:
                writer.write(data)
            await writer.drain()

            status, length, meta = json.loads(await reader.readline())

            return status, meta, await reader.readexactly(length)
        except (ConnectionRefusedError, ConnectionResetError):
            if self.conns[server][1] is not None:
                self.conns[server][1].close()

            self.conns[server] = None, None, None

    async def __call__(self, cmd, meta=None, data=b''):
        servers = self.conns.keys()

        res = await asyncio.gather(
            *[self._rpc(s, cmd, meta, data) for s in servers],
            return_exceptions=True)

        return {s: (r[1], r[2]) for s, r in zip(servers, res)
                if type(r) is tuple and 'OK' == r[0]}


def dump(path, *objects):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp = path + '.' + str(uuid.uuid4()) + '.tmp'
    with open(tmp, 'wb') as fd:
        for obj in objects:
            if type(obj) is not bytes:
                obj = json.dumps(obj, sort_keys=True).encode()

            fd.write(obj)

    os.replace(tmp, path)


def get_max_version(dirname):
    l1_dir = [int(f) for f in os.listdir(dirname) if f.isdigit()]
    for l1 in sorted(l1_dir, reverse=True):
        l2_dirname = os.path.join(dirname, str(l1))
        l2_dir = [int(f) for f in os.listdir(l2_dirname) if f.isdigit()]
        for l2 in sorted(l2_dir, reverse=True):
            l3_dirname = os.path.join(dirname, str(l1), str(l2))
            l2_files = [int(f) for f in os.listdir(l3_dirname) if f.isdigit()]
            for f in sorted(l2_files, reverse=True):
                return f, os.path.join(l3_dirname, str(f))

    return 0, None


def paxos_info(log_id):
    # Directory for this log_id
    h = hashlib.sha256(log_id.encode()).hexdigest()
    logdir = os.path.join('logs', h[0:3], h[3:6], log_id)

    # File that stores the promised seq for upcoming multi-paxos rounds
    promise_filepath = os.path.join(logdir, 'promised')

    # To check if there is already a more recent leader
    promised_seq = 0
    if os.path.isfile(promise_filepath):
        with open(promise_filepath) as fd:
            promised_seq = json.load(fd)['promised_seq']

    return dict(logdir=logdir, promise_filepath=promise_filepath,
                promised_seq=promised_seq)


def paxos_promise(meta, data):
    log_id, proposal_seq = meta

    if os.path.dirname(log_id):
        return 'INVALID_LOG_ID', log_id, None

    info = paxos_info(log_id)

    if proposal_seq <= info['promised_seq']:
        return 'INVALID_PROPOSAL_SEQ', None, None

    # Accept this as the new leader. Any subsequent requests from
    # any stale, older leaders would be rejected
    dump(info['promise_filepath'], dict(promised_seq=proposal_seq))

    max_log_seq, max_filename = get_max_version(info['logdir'])
    if max_log_seq > 0:
        with open(max_filename, 'rb') as fd:
            accepted_seq = json.loads(fd.readline())['accepted_seq']

            return 'OK', [max_log_seq, accepted_seq], fd.read()

    return 'OK', [0, 0], None


def paxos_read(meta, data):
    max_filepath = get_max_version(logdir)
    max_version = int(os.path.basename(max_filepath))
    if max_filepath:
        with open(max_filepath, 'rb') as fd:
            accepted_seq = json.loads(fd.readline())['accepted_seq']

            return 'OK', [max_version, accepted_seq], fd.read()

    return 'OK', [0, 0], None


def paxos_accept(meta, data):
    log_id, log_seq, proposal_seq = meta

    if os.path.dirname(log_id):
        return 'INVALID_LOG_ID', log_id, None

    info = paxos_info(log_id)

    if proposal_seq != info['promised_seq']:
        return 'INVALID_PROPOSAL_SEQ', None, None

    md5 = hashlib.md5(data).hexdigest()
    hdr = dict(log_id=log_id, log_seq=log_seq,
               accepted_seq=proposal_seq, md5=md5)

    path = os.path.join(info['logdir'], str(log_seq // 1000000),
                        str(log_seq // 1000), str(log_seq))

    dump(path, hdr, b'\n', data)

    return 'OK', None, md5.encode()


class Client():
    def __init__(self, servers):
        self.rpc = RPC(servers)
        self.quorum = int(len(servers)/2) + 1

        self.log_seq = None
        self.proposal_seq = None

    async def paxos_promise(self, log_id):
        proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

        res = await self.rpc('promise', [log_id, proposal_seq])

        log_proposal_seq = [[0, 0], b'']
        if len(res) >= self.quorum:
            for meta, data in res.values():
                if meta > log_proposal_seq[0]:
                    log_proposal_seq = [meta, data]

            self.log_seq = log_proposal_seq[0][0]
            self.proposal_seq = proposal_seq

        return await self.paxos_propose(log_id, log_proposal_seq[1])

    async def paxos_propose(self, log_id, blob):
        paxos = [log_id, self.log_seq, self.proposal_seq]

        if self.quorum > len(await self.rpc('accept', paxos, blob)):
            self.log_seq = self.proposal_seq = None
            return 'NO_QUORUM', None

        self.log_seq += 1
        return 'OK', self.log_seq - 1

    async def append(self, log_id, blob):
        if self.log_seq is None:
            await self.paxos_promise(log_id)

        timestamp = time.time()

        status, log_seq = await self.paxos_propose(log_id, blob)

        return dict(msec=int((time.time() - timestamp)*1000),
                    log_id=log_id, log_seq=log_seq, status=status)

    async def tail(self, log_id, log_seq):
        pass


async def run_server(port):
    server = await asyncio.start_server(request_handler, None, port)
    async with server:
        await server.serve_forever()


if '__main__' == __name__:
    if 2 == len(sys.argv):
        # Server
        logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
        asyncio.run(run_server(int(sys.argv[1])))

    elif 3 == len(sys.argv):
        # CLI
        client = Client(sys.argv[1].split(','))

        r = asyncio.run(client.append(sys.argv[2], sys.stdin.buffer.read()))
        print(r)

        exit(0) if 'OK' == r['status'] is int else exit(1)
