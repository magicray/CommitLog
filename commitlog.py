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
            # traceback.print_exc()
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


def get_logdir(log_id):
    h = hashlib.sha256(log_id.encode()).hexdigest()
    return os.path.join('logs', h[0:3], h[3:6], log_id)


def get_logfile(log_id, log_seq):
    l1, l2, = log_seq//1000000, log_seq//1000
    return os.path.join(get_logdir(log_id), str(l1), str(l2), str(log_seq))


def max_file(log_id):
    logdir = get_logdir(log_id)

    if not os.path.isdir(logdir):
        return 0, None

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

    path = get_logfile(log_id, log_seq)

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

    logdir = get_logdir(log_id)
    promise_filepath = os.path.join(logdir, 'promised')

    promised_seq = 0
    if os.path.isfile(promise_filepath):
        with open(promise_filepath) as fd:
            obj = json.load(fd)
            uuid = obj['uuid']
            promised_seq = obj['promised_seq']

    # Accept this as the new leader as it has higher proposal_seq.
    # Subsequent requests from any stale, older leaders would be rejected
    if proposal_seq > promised_seq:
        uuid = guid
        promised_seq = proposal_seq
        dump(promise_filepath, dict(promised_seq=proposal_seq, uuid=guid))

    # paxos PROMISE phase
    # Return the most recent accepted value.
    # Client will take the most recent of these, across servers and
    # propose that in the ACCEPT phase.
    if 'promise' == phase and proposal_seq == promised_seq and guid == uuid:
        # Most recent file in this log stream
        log_seq, filepath = max_file(log_id)

        # Log stream does not yet exist
        if 0 == log_seq:
            return 'OK', [0, 0]

        with open(filepath, 'rb') as fd:
            obj = json.loads(fd.readline())
            md5_chain = obj['md5_chain']
            accepted_seq = obj['accepted_seq']
            return 'OK', [log_seq, accepted_seq, md5_chain], fd.read()

    # paxos ACCEPT phase
    # Safe to accept as only the most recent leader can reach this stage
    if 'accept' == phase and proposal_seq == promised_seq and guid == uuid:
        log_seq, md5_chain = meta[3], meta[4]

        md5 = hashlib.md5(data).hexdigest()
        obj = dict(log_id=log_id, log_seq=log_seq, accepted_seq=proposal_seq,
                   md5=md5, md5_chain=md5_chain, uuid=uuid, length=len(data))

        hdr = json.dumps(obj, sort_keys=True).encode()

        dump(get_logfile(log_id, log_seq), hdr, b'\n', data)

        return 'OK', hashlib.md5(hdr).hexdigest()

    return 'STALE_PROPOSAL_SEQ', proposal_seq


class Client():
    def __init__(self, cert, servers, quorum=0):
        self.rpc = RPC(cert, servers)
        self.logs = dict()
        self.quorum = max(quorum, int(len(servers)/2) + 1)

    async def lead(self, log_id):
        return await self.append(log_id, None)

    async def append(self, log_id, blob):
        if log_id not in self.logs and blob is None:
            # paxos PROMISE phase - block stale leaders from writing
            guid = str(uuid.uuid4())
            proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

            res = await self.rpc('promise', [log_id, proposal_seq, guid])
            if self.quorum > len(res):
                return

            # Format = [[log_seq, accepted_seq, md5], blob]
            proposal = [[0, 0, str(uuid.uuid4())], str(uuid.uuid4()).encode()]
            for meta, data in res.values():
                if meta > proposal[0]:
                    proposal = [meta, data]

            # Last write from the previous leader might have failed.
            # This client should complete that before any new writes.
            log_seq, md5, blob = proposal[0][0], proposal[0][2], proposal[1]

        elif blob is not None:
            # Take away leadership temporarily.
            proposal_seq, guid, log_seq, md5 = self.logs.pop(log_id)

        if not blob:
            return

        # paxos ACCEPT phase - write a new blob
        # Retry a few times to overcome temp failures
        for delay in (1, 1, 1, 1, 1, 0):
            meta = [log_id, proposal_seq, guid, log_seq, md5]
            res = await self.rpc('accept', meta, blob)

            if self.quorum > len(res):
                await asyncio.sleep(delay)
                continue

            md5 = set([meta for meta, data in res.values()])
            if 1 == len(md5):
                # Write successful. Reinstate as the leader.
                md5 = md5.pop()
                self.logs[log_id] = [proposal_seq, guid, log_seq+1, md5]
                return dict(log_seq=log_seq, md5=md5)

    async def tail(self, log_id, seq, wait_sec=1):
        max_seq = seq
        md5_chain = None

        while True:
            res = await self.rpc('logseq', log_id)
            if len(res) >= self.quorum:
                max_seq = max([v[0] for v in res.values()])

            while seq < max_seq:
                res = await self.rpc('read', ['meta', log_id, seq])
                if len(res) >= self.quorum:
                    srv = None
                    accepted_seq = 0
                    for server, res in res.items():
                        if res[0]['accepted_seq'] > accepted_seq:
                            srv = server
                            accepted_seq = res[0]['accepted_seq']

                if srv:
                    res = await self.rpc('read', ['data', log_id, seq],
                                         server=srv)
                if not res:
                    log('waiting for quorum')
                    await asyncio.sleep(wait_sec)
                    continue

                meta, data = res[srv]
                if md5_chain and md5_chain != meta['md5_chain']:
                    log(('FATAL chain mismatch', md5_chain, meta['md5_chain']))
                    return

                hdr = json.dumps(meta, sort_keys=True).encode()
                md5_chain = hashlib.md5(hdr).hexdigest()

                yield meta, data
                seq = seq + 1

            log('waiting for quorum or data')
            await asyncio.sleep(wait_sec)


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    # Server
    if len(sys.argv) < 4:
        cert, port = sys.argv[1], int(sys.argv[2])

        SSL = ssl.create_default_context(
            cafile=cert,
            purpose=ssl.Purpose.CLIENT_AUTH)
        SSL.load_cert_chain(cert, cert)
        SSL.verify_mode = ssl.CERT_REQUIRED

        srv = await asyncio.start_server(server, None, port, ssl=SSL)
        async with srv:
            return await srv.serve_forever()

    # Tail
    elif sys.argv[-1].isdigit():
        cert, servers = sys.argv[1], sys.argv[2:-2]
        log_id, log_seq = sys.argv[-2], int(sys.argv[-1])

        client = Client(cert, servers)

        async for meta, data in client.tail(log_id, log_seq):
            log((meta, len(data)))

    # Append
    else:
        cert, servers, log_id = sys.argv[1], sys.argv[2:-1], sys.argv[-1]

        client = Client(cert, servers)

        log(await client.lead(log_id))

        while True:
            blob = sys.stdin.buffer.read(1024*1024)
            if not blob:
                exit(0)

            result = await client.append(log_id, blob)

            log(result) if result else exit(1)


if '__main__' == __name__:
    asyncio.run(main())
