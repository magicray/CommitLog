import os
import sys
import json
import time
import uuid
import hashlib
import asyncio
import logging
from logging import critical as log

logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')


async def request_handler(reader, writer):
    HANDLERS = dict(paxos=paxos_server)

    peer = writer.get_extra_info('socket').getpeername()

    while True:
        try:
            try:
                cmd, req_length, req_hdr = json.loads(await reader.readline())
            except Exception:
                log('req{} disconnected or invalid header'.format(peer))
                return writer.close()

            if cmd not in ('paxos', 'dump'):
                log('req{} {} invalid command'.format(peer, cmd))
                return writer.close()

            res_hdr, data = HANDLERS[cmd](
                req_hdr,
                await reader.readexactly(req_length))

            res_length = len(data) if data else 0

            writer.write(json.dumps([res_length, res_hdr]).encode())
            writer.write(b'\n')
            if res_length > 0:
                writer.write(data)

            await writer.drain()
            log(('peer{} {} req_length({}) req_header({}) ' +
                 'res_length({}) res_header({})').format(
                peer, cmd, req_length, req_hdr, res_length, res_hdr))
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

            length, meta = json.loads(await reader.readline())

            return meta, await reader.readexactly(length)
        except (ConnectionRefusedError, ConnectionResetError):
            if self.conns[server][1] is not None:
                self.conns[server][1].close()

            self.conns[server] = None, None

    async def __call__(self, cmd, meta=None, data=None):
        servers = self.conns.keys()

        res = await asyncio.gather(
            *[self._rpc(s, cmd, meta, data) for s in servers],
            return_exceptions=True)

        return {s: r for s, r in zip(servers, res) if r}


def dump(path, blobs):
        os.makedirs(os.path.dirname(path), exist_ok=True)

        tmp = path + '.' + str(uuid.uuid4()) + '.tmp'
        with open(tmp, 'wb') as fd:
            for blob in blobs:
                fd.write(blob)
        os.replace(tmp, path)


def get_dirname(log_id, log_seq):
    if os.path.basename(log_id) != log_id:
        return

    h = hashlib.sha256(log_id.encode()).hexdigest()
    x = os.path.join('logs', h[0:3], h[3:6], log_id)
    return os.path.join(x, str(log_seq // 1000000), str(log_seq // 1000))


def paxos_server(meta, data):
    phase, log_id, log_seq = meta['phase'], meta['log_id'], meta['log_seq']

    dirname = get_dirname(log_id, log_seq)
    filename = os.path.join(dirname, str(log_seq))
    
    proposal_seq = meta['proposal_seq']

    promised_seq = accepted_seq = 0
    if os.path.isfile(filename):
        with open(filename, 'rb') as fd:
            obj = json.loads(fd.readline())

            if 'md5' in obj:
                # This value is already learned
                # Just pretend to participate in paxos rounds,
                # without modifying anything
                if 'promise' == phase:
                    return dict(accepted_seq=99999999999999), fd.read()

                return 'OK', None

            promised_seq = obj['promised_seq']
            accepted_seq = obj['accepted_seq']

    if 'promise' == phase and proposal_seq > promised_seq:
        dump(filename, [json.dumps(dict(
            promised_seq=proposal_seq,
            accepted_seq=accepted_seq), sort_keys=True).encode()])

        if 0 == accepted_seq:
            return dict(accepted_seq=0), b''

        with open(filename + '.' + str(accepted_seq), 'rb') as fd:
            ignore_this = fd.readline()
            return dict(accepted_seq=accepted_seq), fd.read()

    if 'accept' == phase and proposal_seq == promised_seq:
        hdr = json.dumps(dict(md5=hashlib.md5(data).hexdigest()),
                         sort_keys=True).encode()

        dump(filename + '.' + str(proposal_seq), [hdr, b'\n', data])

        dump(filename, [json.dumps(dict(
            promised_seq=proposal_seq,
            accepted_seq=proposal_seq), sort_keys=True).encode()])

        return 'OK', None

    if 'learn' == phase and proposal_seq == promised_seq == accepted_seq:
        os.rename(filename + '.' + str(proposal_seq), filename)
        return 'OK', None

    return 'INVALID_PROPOSAL_SEQ', None


async def paxos_client(rpc, quorum, log_id, log_seq, blob):
    # paxos seq is an integer in the following format - YYYYmmddHHMMSS
    # This would increase monotonically. Even if same seq is generated by
    # more than one instances of paxos rounds, protocol handles it and rejects
    # the later round (as proposal_seq should be GREATER than the promised_seq)
    paxos = dict(
        log_id=log_id,
        log_seq=log_seq,
        proposal_seq=int(time.strftime('%Y%m%d%H%M%S')))

    paxos['phase'] = 'promise'
    res = await rpc('paxos', paxos)
    if quorum > len(res):
        return 'NO_PROMISE_QUORUM'

    # Find out the accepted value with the highest accepted_seq
    proposal = (0, blob)
    for meta, data in res.values():
        if meta['accepted_seq'] > proposal[0]:
            proposal = (meta['accepted_seq'], data)

    if not proposal[1]:
        return 'EMPTY_BLOB'

    paxos['phase'] = 'accept'
    res = await rpc('paxos', paxos, proposal[1])
    if quorum > len(res):
        return 'NO_ACCEPT_QUORUM'

    paxos['phase'] = 'learn'
    res = await rpc('paxos', paxos)
    if quorum > len(res):
        return 'NO_LEARN_QUORUM'

    return 'OK' if 0 == proposal[0] else 'CONFLICT'


class Client():
    def __init__(self, servers):
        self.rpc = RPC(servers)
        self.quorum = int(len(servers)/2) + 1

    async def write(self, log_id, log_seq, blob):
        status = await paxos_client(self.rpc, self.quorum,
                                    log_id, log_seq, blob)
        return status

    async def read(self, log_id, log_seq):
        pass


async def server(port):
    server = await asyncio.start_server(request_handler, None, port)
    async with server:
        await server.serve_forever()


if '__main__' == __name__:
    # Start the server on th
    if 2 == len(sys.argv):
        asyncio.run(server(int(sys.argv[1])))
    elif 4 == len(sys.argv):
        client = Client(sys.argv[1].split(','))
        status = asyncio.run(client.write(sys.argv[2], int(sys.argv[3]), b'Hello'))
        print(status)
