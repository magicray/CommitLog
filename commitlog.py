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
    HANDLERS = dict(paxos=paxos_server)

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

    async def _rpc(self, server, cmd, meta=None, data=None):
        try:
            if self.conns[server][0] is None or self.conns[server][1] is None:
                self.conns[server] = await asyncio.open_connection(
                    server[0], server[1])

            reader, writer = self.conns[server]

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

    async def __call__(self, cmd, meta=None, data=None):
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


def paxos_server(meta, data):
    phase, log_id, log_seq, proposal_seq = meta

    if os.path.dirname(log_id):
        return 'INVALID_LOG_ID', log_id, None

    # Directory for this log_id
    h = hashlib.sha256(log_id.encode()).hexdigest()
    logdir = os.path.join('logs', h[0:3], h[3:6], log_id)

    # File that stores the promised seq for upcoming multi-paxos rounds
    promise_filepath = os.path.join(logdir, 'promised')

    # File that stores the actual data
    log_filepath = os.path.join(logdir, str(log_seq // 1000000),
                                str(log_seq // 1000), str(log_seq))

    # Check if there is already a more recent leader
    promised_seq = 0
    if os.path.isfile(promise_filepath):
        with open(promise_filepath) as fd:
            promised_seq = json.load(fd)['promised_seq']

    # This requestor wants to become the new multi-paxos leader
    if 'promise' == phase and proposal_seq > promised_seq:
        # Accept this as the new leader. Any subsequent requests from
        # any stale, older leaders would be rejected
        dump(promise_filepath, dict(promised_seq=proposal_seq))

        # Return if a value for this log_seq was already accepted.
        # Client is supposed to pick the one with the most recent accepted_seq
        # and use that in the ACCEPT phase later.
        if os.path.isfile(log_filepath):
            with open(log_filepath, 'rb') as fd:
                accepted_seq = json.loads(fd.readline())['accepted_seq']

                return 'OK', accepted_seq, fd.read()

        return 'OK', 0, None

    # Most request would be only this. PROMISE phase is run once by a leader.
    # This value is either the most recent returned to client in the PROMISE
    # phase or the value proposed by the client if no value was already
    # accepted and returned in the PROMISE phase.
    if 'accept' == phase and proposal_seq == promised_seq:
        md5 = hashlib.md5(data).hexdigest()
        hdr = dict(log_id=log_id, log_seq=log_seq,
                   accepted_seq=proposal_seq,
                   md5=md5)
        dump(log_filepath, hdr, b'\n', data)

        return 'OK', None, md5.encode()

    return 'INVALID_PROPOSAL_SEQ', None, None


async def paxos_client(rpc, quorum, log_id, log_seq, blob, proposal_seq=None):
    paxos = [None, log_id, log_seq, proposal_seq]
    proposal = (0, blob)

    if proposal_seq is None:
        # Run PROMISE phase once for a leader,
        # only ACCEPT phase is needed for later rounds.
        paxos = [None, log_id, log_seq, int(time.strftime('%Y%m%d%H%M%S'))]

        paxos[0] = 'promise'
        res = await rpc('paxos', paxos)
        if quorum > len(res):
            return dict(status='NO_QUORUM', proposal_seq=paxos[3])

        # Find out the accepted value with the highest accepted_seq
        for accepted_seq, data in res.values():
            if accepted_seq > proposal[0]:
                proposal = (accepted_seq, data)

    if not proposal[1]:
        return dict(status='EMPTY_BLOB', proposal_seq=paxos[3])

    paxos[0] = 'accept'
    if quorum > len(await rpc('paxos', paxos, proposal[1])):
        return dict(status='NO_QUORUM', proposal_seq=paxos[3])

    return dict(proposal_seq=paxos[3],
                status='OK' if 0 == proposal[0] else 'CONFLICT')


class Client():
    def __init__(self, servers):
        self.rpc = RPC(servers)
        self.quorum = int(len(servers)/2) + 1

    async def append(self, log_id, log_seq, blob):
        ts = time.time()
        while True:
            result = await paxos_client(
                self.rpc, self.quorum, log_id, log_seq, blob)

            if 'NO_QUORUM' != result['status']:
                break

            await asyncio.sleep(1)

        result['log_id'] = log_id
        result['log_seq'] = log_seq
        result['msec'] = int((time.time() - ts)*1000)

        return result

    async def tail(self, log_id, log_seq):
        pass


async def run_server(port):
    server = await asyncio.start_server(request_handler, None, port)
    async with server:
        await server.serve_forever()


def main():
    if 2 == len(sys.argv):
        # Server
        logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
        asyncio.run(run_server(int(sys.argv[1])))

    elif 4 == len(sys.argv):
        # CLI
        servers = sys.argv[1].split(',')
        log_id, log_seq = sys.argv[2], int(sys.argv[3])

        blob = sys.stdin.buffer.read()
        result = asyncio.run(Client(servers).append(log_id, log_seq, blob))
        print(result)

        exit(0) if 'OK' == result['status'] else exit(1)


if '__main__' == __name__:
    main()
