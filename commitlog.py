import os
import sys
import ssl
import json
import time
import uuid
import hashlib
import asyncio
import logging
import traceback
from logging import critical as log


async def server(reader, writer):
    HANDLERS = dict(promise=paxos_server, accept=paxos_server,
                    logseq=logseq_server, read=read_server)

    peer = writer.get_extra_info('socket').getpeername()

    while True:
        try:
            try:
                req = await reader.readline()
                if not req:
                    return writer.close()

                req = req.decode().strip()
                cmd, hdr, length = json.loads(req)
            except Exception:
                log(f'{peer} disconnected or invalid header')
                return writer.close()

            if cmd not in HANDLERS:
                log(f'{peer} invalid command {req}')
                return writer.close()

            try:
                result = HANDLERS[cmd](hdr, await reader.readexactly(length))
                if type(result) is str:
                    result = (result, None, None)
                status, hdr, data = list(result) + [None] * (3 - len(result))
            except Exception as e:
                traceback.print_exc()
                status, hdr, data = 'EXCEPTION', str(e), None

            length = len(data) if data else 0
            res = json.dumps([status, hdr, length])

            writer.write(res.encode())
            writer.write(b'\n')
            if length > 0:
                writer.write(data)

            await writer.drain()
            log(f'{peer} {cmd}:{status} {req} {res}')
        except Exception as e:
            traceback.print_exc()
            log(f'{peer} FATAL({e})')
            os._exit(0)


class RPC():
    def __init__(self, cert, servers):
        self.SSL = ssl.create_default_context(
            cafile=cert,
            purpose=ssl.Purpose.SERVER_AUTH)
        self.SSL.load_cert_chain(cert, cert)
        self.SSL.verify_mode = ssl.CERT_REQUIRED

        self.conns = dict()

        for srv in servers:
            ip, port = srv.split(':')
            self.conns[(ip, int(port))] = None, None

    async def _rpc(self, server, cmd, meta=None, data=b''):
        try:
            if self.conns[server][0] is None or self.conns[server][1] is None:
                self.conns[server] = await asyncio.open_connection(
                    server[0], server[1], ssl=self.SSL)

            reader, writer = self.conns[server]

            if data and type(data) is not bytes:
                try:
                    data = json.dumps(data).encode()
                except Exception as e:
                    data = str(e).encode()

            length = len(data) if data else 0

            writer.write(json.dumps([cmd, meta, length]).encode())
            writer.write(b'\n')
            if length > 0:
                writer.write(data)
            await writer.drain()

            status, meta, length = json.loads(await reader.readline())

            return status, meta, await reader.readexactly(length)
        except Exception as e:
            traceback.print_exc()
            log(e)
            if self.conns[server][1] is not None:
                self.conns[server][1].close()

            self.conns[server] = None, None, None

    async def __call__(self, cmd, meta=None, data=b'', server=None):
        servers = self.conns.keys() if server is None else [server]

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


def max_file(log_id):
    # Directory for this log_id
    h = hashlib.sha256(log_id.encode()).hexdigest()
    logdir = os.path.join('logs', h[0:3], h[3:6], log_id)

    # Get max log_seq for this log_id
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
                return f, os.path.join(l3_dirname, str(f))

    return 0, None


def logseq_server(meta, data):
    return 'OK', max_file(meta)[0]


def read_server(meta, data):
    what, log_id, log_seq = meta

    h = hashlib.sha256(log_id.encode()).hexdigest()
    l1, l2, = log_seq//1000000, log_seq//1000

    path = os.path.join(
        'logs', h[0:3], h[3:6], log_id,
        str(l1), str(l2), str(log_seq))

    if os.path.isfile(path):
        with open(path, 'rb') as fd:
            meta = json.loads(fd.readline())

            if 'data' == what:
                return 'OK', meta, fd.read()

            return 'OK', meta

    return 'NOTFOUND'


def paxos_server(meta, data):
    phase = 'promise' if 3 == len(meta) else 'accept'
    log_id, proposal_seq, guid = meta[0], meta[1], meta[2]

    if os.path.dirname(log_id):
        return 'INVALID_LOG_ID', log_id

    # Directory for this log_id
    h = hashlib.sha256(log_id.encode()).hexdigest()
    logdir = os.path.join('logs', h[0:3], h[3:6], log_id)

    # File that stores the promised seq for upcoming multi-paxos rounds
    promise_filepath = os.path.join(logdir, 'promised')

    # To check if there is already a more recent leader
    promised_seq = 0
    if os.path.isfile(promise_filepath):
        with open(promise_filepath) as fd:
            obj = json.load(fd)
            uuid = obj['uuid']
            promised_seq = obj['promised_seq']

    # Accept this as the new leader. Any subsequent requests from
    # any stale, older leaders would be rejected
    if proposal_seq > promised_seq:
        uuid = guid
        promised_seq = proposal_seq
        dump(promise_filepath, dict(promised_seq=proposal_seq, uuid=guid))

    if 'promise' == phase and proposal_seq == promised_seq and guid == uuid:
        log_seq, filepath = max_file(log_id)

        if 0 == log_seq:
            return 'OK', [0, 0]

        with open(filepath, 'rb') as fd:
            accepted_seq = json.loads(fd.readline())['accepted_seq']
            return 'OK', [log_seq, accepted_seq], fd.read()

    if 'accept' == phase and proposal_seq == promised_seq and guid == uuid:
        log_seq = meta[3]

        md5 = hashlib.md5(data).hexdigest()
        hdr = dict(log_id=log_id, log_seq=log_seq, accepted_seq=proposal_seq,
                   md5=md5, uuid=uuid, length=len(data))

        l1, l2, = log_seq//1000000, log_seq//1000
        path = os.path.join(logdir, str(l1), str(l2), str(log_seq))

        dump(path, hdr, b'\n', data)

        return 'OK', md5

    return 'STALE_PROPOSAL_SEQ', proposal_seq


class Client():
    def __init__(self, cert, servers, quorum=0):
        self.rpc = RPC(cert, servers)
        self.logs = dict()
        self.quorum = max(quorum, int(len(servers)/2) + 1)

    async def append(self, log_id, blob):
        ts = time.time()

        if log_id not in self.logs:
            # paxos PROMISE phase - block stale leaders from writing
            guid = str(uuid.uuid4())
            proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

            res = await self.rpc('promise', [log_id, proposal_seq, guid])
            if self.quorum > len(res):
                return dict(status=False, msec=int((time.time() - ts) * 1000))

            # Format = [[log_seq, accepted_seq], blob]
            proposal = [[0, 0], b'']
            for meta, data in res.values():
                if meta > proposal[0]:
                    proposal = [meta, data]

            # paxos ACCEPT phase - Write the latest value retrieved earlier
            # Last write from the previous leader might have failed.
            # This client should complete that before any new writes.
            log_seq, data = proposal[0][0], proposal[1]

            meta = [log_id, proposal_seq, guid, proposal[0][0]]
            res = await self.rpc('accept', meta, data)
            if self.quorum > len(res):
                return dict(status=False, msec=int((time.time() - ts) * 1000))

            md5 = set([meta for meta, data in res.values()])
            if 1 != len(md5):
                return dict(status=False, msec=int((time.time() - ts) * 1000))

            # This client is THE LEADER now
            # Write successfuly completed. Set next log_seq = log_seq + 1
            self.logs[log_id] = [proposal_seq, guid, log_seq+1]

            if not blob:
                return dict(status=True, log_seq=log_seq, md5=md5.pop(),
                            msec=int((time.time() - ts) * 1000))

        if not blob:
            return dict(status=True, msec=int((time.time() - ts) * 1000))

        # Take away leadership temporarily.
        proposal_seq, guid, log_seq = self.logs.pop(log_id)

        # Write the blob. Retry a few times to overcome temp failures
        for delay in (1, 1, 1, 1, 1, 0):
            meta = [log_id, proposal_seq, guid, log_seq]
            res = await self.rpc('accept', meta, data)

            if self.quorum > len(res):
                await asyncio.sleep(delay)
                continue
            else:
                md5 = set([meta for meta, data in res.values()])
                if 1 != len(md5):
                    await asyncio.sleep(delay)
                    continue

                # Commit successful. Reinstate as the leader.
                self.logs[log_id] = [proposal_seq, guid, log_seq+1]

                return dict(status=True, log_seq=log_seq, md5=md5.pop(),
                            msec=int((time.time() - ts) * 1000))

        return dict(status=False, msec=int((time.time() - ts) * 1000))

    async def tail(self, log_id, start_seq, wait_sec=1):
        while True:
            res = await self.rpc('logseq', log_id)
            if self.quorum > len(res):
                log(f'NO QUORUM start_seq({start_seq})')
                await asyncio.sleep(wait_sec)
                continue

            max_seq = max([v[0] for v in res.values()])
            if max_seq <= start_seq:
                log(f'NO QUORUM start_seq({start_seq}) max_seq({max_seq})')
                await asyncio.sleep(wait_sec)
                continue

            for seq in range(start_seq, max_seq):
                while True:
                    res = await self.rpc('read', ['meta', log_id, seq])
                    if self.quorum > len(res):
                        log(f'NO QUORUM seq({seq}) max_seq({max_seq})')
                        await asyncio.sleep(wait_sec)
                        continue

                    srv = None
                    accepted_seq = 0
                    for server, res in res.items():
                        if res[0]['accepted_seq'] > accepted_seq:
                            srv = server
                            accepted_seq = res[0]['accepted_seq']

                    res = await self.rpc('read', ['data', log_id, seq],
                                         server=srv)
                    if not res:
                        log(f'NO QUORUM seq({seq}) max_seq({max_seq})')
                        await asyncio.sleep(wait_sec)
                        continue

                    yield res[srv]
                    start_seq = seq + 1
                    break


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    # Server
    if len(sys.argv) < 4:
        cert = sys.argv[1]
        port = int(sys.argv[2])

        SSL = ssl.create_default_context(
            cafile=cert,
            purpose=ssl.Purpose.CLIENT_AUTH)
        SSL.load_cert_chain(cert, cert)
        SSL.verify_mode = ssl.CERT_REQUIRED

        srv = await asyncio.start_server(server, None, port, ssl=SSL)
        async with srv:
            await srv.serve_forever()

    # Tail
    elif sys.argv[-1].isdigit():
        client = Client(sys.argv[1], sys.argv[2:-2])

        log_id = sys.argv[-2]
        log_seq = int(sys.argv[-1])

        async for meta, data in client.tail(log_id, log_seq):
            log((meta, len(data)))

    # Append
    else:
        client = Client(sys.argv[1], sys.argv[2:-1])

        while True:
            blob = sys.stdin.buffer.read(1024*1024)
            if not blob:
                exit(0)

            result = await client.append(sys.argv[-1], blob)
            log(result)

            if result['status'] is not True:
                exit(1)


if '__main__' == __name__:
    asyncio.run(main())
