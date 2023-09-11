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
    HANDLERS = dict(promise=paxos_server, accept=paxos_server)

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
                log('{} disconnected or invalid header'.format(peer))
                return writer.close()

            if cmd not in HANDLERS:
                log('{} invalid command {}'.format(peer, req))
                return writer.close()

            try:
                status, hdr, data = HANDLERS[cmd](
                    hdr,
                    await reader.readexactly(length))
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
            log('{} {}:{} {} {}'.format(peer, cmd, status, req, res))
        except Exception as e:
            traceback.print_exc()
            log('{} FATAL({})'.format(peer, e))
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

            writer.write(json.dumps([cmd, meta, length]).encode())
            writer.write(b'\n')
            if length > 0:
                writer.write(data)
            await writer.drain()

            status, meta, length = json.loads(await reader.readline())

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
    # os.sync()


def paxos_server(meta, data):
    phase = 'promise' if 3 == len(meta) else 'accept'
    log_id, proposal_seq, guid = meta[0], meta[1], meta[2]

    if os.path.dirname(log_id):
        return 'INVALID_LOG_ID', log_id, None

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

    if 'promise' == phase and proposal_seq > promised_seq:
        # Accept this as the new leader. Any subsequent requests from
        # any stale, older leaders would be rejected
        dump(promise_filepath, dict(promised_seq=proposal_seq, uuid=guid))

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
                    log_file = os.path.join(l3_dirname, str(f))

                    with open(log_file, 'rb') as fd:
                        seq = json.loads(fd.readline())['accepted_seq']
                        return 'OK', [f, seq], fd.read()

        return 'OK', [0, 0], None

    if 'accept' == phase and proposal_seq == promised_seq and guid == uuid:
        log_seq = meta[3]

        md5 = hashlib.md5(data).hexdigest()
        hdr = dict(log_id=log_id, log_seq=log_seq, md5=md5, length=len(data),
                   uuid=uuid, accepted_seq=proposal_seq)

        l1, l2, = log_seq//1000000, log_seq//1000
        path = os.path.join(logdir, str(l1), str(l2), str(log_seq))

        dump(path, hdr, b'\n', data)

        return 'OK', md5, None

    return 'INVALID_PROPOSAL_SEQ', proposal_seq, str(promised_seq).encode()


class Client():
    def __init__(self, servers, quorum=0):
        self.rpc = RPC(servers)
        self.logs = dict()
        self.quorum = max(quorum, int(len(servers)/2) + 1)

    async def paxos_promise(self, log_id):
        guid = str(uuid.uuid4())
        proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

        meta = [log_id, proposal_seq, guid]
        res = await self.rpc('promise', meta)
        if self.quorum > len(res):
            # Can't decide without hearing back from a quorum
            return 'NO_QUORUM'

        # Format = [[log_seq, accepted_seq], blob]
        proposal = [[0, 0], b'']

        for meta, data in res.values():
            if meta > proposal[0]:
                proposal = [meta, data]

        # This blob should be written again for this log_seq with this
        # proposal_seq, as it might not have been successfully written
        # on a quorum of nodes when last leader died or evicted.
        #
        # Returns - log_seq, proposal_seq, uuid, blob
        return proposal[0][0], proposal_seq, guid, proposal[1]

    async def paxos_accept(self, log_id, proposal_seq, guid, log_seq, blob):
        meta = [log_id, proposal_seq, guid, log_seq]

        for delay in (0.5, 0.5, 1, 1, 1, 2, 2, 2, 0):
            res = await self.rpc('accept', meta, blob)

            if len(res) >= self.quorum:
                # blob is successfully written to a quorum of servers
                return 'OK'

            await asyncio.sleep(delay)

    async def append(self, log_id, blob):
        timestamp = time.time()

        if log_id not in self.logs:
            log_seq, proposal_seq, guid, old = await self.paxos_promise(log_id)

            res = await self.paxos_accept(
                log_id, proposal_seq, guid, log_seq, old)

            if 'OK' == res:
                # This instance is the new leader for this log -:)
                self.logs[log_id] = [proposal_seq, guid, log_seq+1]

                if not blob:
                    return dict(log_id=log_id, log_seq=log_seq, blob=old,
                                status='LEADER',
                                msec=int((time.time() - timestamp) * 1000))

        if log_id not in self.logs:
            return dict(log_id=log_id, status='NOT_LEADER',
                        msec=int((time.time() - timestamp) * 1000))

        # Get the next log_seq and proposal_seq for this leader
        proposal_seq, guid, log_seq = self.logs[log_id]

        status = await self.paxos_accept(
            log_id, proposal_seq, guid, log_seq, blob)

        if 'OK' == status:
            # Update the log_seq - value where next blob would be written
            self.logs[log_id] = [proposal_seq, guid, log_seq+1]

            return dict(log_id=log_id, log_seq=log_seq, blob=blob,
                        status=status,
                        msec=int((time.time() - timestamp) * 1000))

        return dict(log_id=log_id, status='FAILED',
                    msec=int((time.time() - timestamp) * 1000))

    async def tail(self, log_id, log_seq):
        pass


async def run_server(port):
    server = await asyncio.start_server(request_handler, None, port)
    async with server:
        await server.serve_forever()


if '__main__' == __name__:
    if len(sys.argv) < 3:
        logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
        asyncio.run(run_server(int(sys.argv[1])))

    else:
        loop = asyncio.get_event_loop()
        client = Client(sys.argv[1:-1])

        while True:
            blob = sys.stdin.buffer.read(1024*1024)
            if not blob:
                exit(0)

            result = loop.run_until_complete(client.append(sys.argv[-1], blob))
            result.pop('blob')
            print(result)

            if 'OK' != result['status']:
                exit(1)
