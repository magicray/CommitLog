import ssl
import json
import uuid
import asyncio
from logging import critical as log


class RPC():
    def __init__(self, cert, servers):
        self.SSL = ssl.create_default_context(
            cafile=cert,
            purpose=ssl.Purpose.SERVER_AUTH)
        self.SSL.load_cert_chain(cert, cert)
        self.SSL.verify_mode = ssl.CERT_REQUIRED
        self.SSL.check_hostname = False

        self.conns = {tuple(srv): (None, None) for srv in servers}

    async def stream(self, server, method, header):
        try:
            reader, writer = await asyncio.open_connection(
                server[0], server[1], ssl=self.SSL)

            writer.write(json.dumps([method, header, 0]).encode())
            writer.write(b'\n')
            await writer.drain()

            while True:
                status, header, length = json.loads(await reader.readline())
                yield status, header, await reader.readexactly(length)
        except Exception as e:
            log(e)

    async def rpc(self, server, method, header=None, body=b''):
        try:
            if self.conns[server][0] is None or self.conns[server][1] is None:
                self.conns[server] = await asyncio.open_connection(
                    server[0], server[1], ssl=self.SSL)

            reader, writer = self.conns[server]

            if body and type(body) is not bytes:
                body = json.dumps(body).encode()

            length = len(body) if body else 0

            writer.write(json.dumps([method, header, length]).encode())
            writer.write(b'\n')
            if length > 0:
                writer.write(body)
            await writer.drain()

            status, header, length = json.loads(await reader.readline())

            return status, header, await reader.readexactly(length)
        except Exception as e:
            log(e)
            if self.conns[server][1] is not None:
                self.conns[server][1].close()

            self.conns[server] = None, None

    async def __call__(self, method, header=None, body=b''):
        servers = self.conns.keys()

        res = await asyncio.gather(
            *[self.rpc(s, method, header, body) for s in servers],
            return_exceptions=True)

        return {s: (r[1], r[2]) for s, r in zip(servers, res)
                if type(r) is tuple and 'OK' == r[0]}

    def __del__(self):
        for server, (reader, writer) in self.conns.items():
            if writer is not None:
                writer.close()


class Client():
    def __init__(self, cert, servers):
        self.rpc = RPC(cert, servers)
        self.quorum = int(len(servers)/2) + 1
        self.servers = servers

        self.log_seq = None
        self.proposal_seq = None

    async def lead(self):
        self.log_seq = self.proposal_seq = None

        # Server would run PROMISE phase on this client's behalf
        res = await self.rpc('grant', self.servers)
        result = sorted([hdr for hdr, body in res.values()], reverse=True)

        if not result:
            raise Exception('ELECTION_FAILED')

        self.proposal_seq, self.log_seq = result[0]

        return self.log_seq

    async def commit(self, blob):
        if not blob:
            raise Exception('EMPTY_REQUEST')

        if not self.proposal_seq:
            raise Exception('NOT_THE_LEADER')

        # Remove as the leader
        proposal_seq = self.proposal_seq
        self.proposal_seq = None

        # paxos ACCEPT phase - write a new blob
        hdr = [proposal_seq, self.log_seq + 1, str(uuid.uuid4())]
        res = await self.rpc('accept', hdr, blob)
        hdrs = {json.dumps(h, sort_keys=True) for h, _ in res.values()}

        # Reinstate the leader as the write is successful.
        if len(res) >= self.quorum and 1 == len(hdrs):
            self.log_seq += 1
            return json.loads(hdrs.pop())

        raise Exception(f'NO_QUORUM log_seq({self.log_seq})')

    async def tail(self, seq):
        while True:
            for server in self.servers:
                header = [seq, self.servers]
                async for response in self.rpc.stream(server, 'tail', header):
                    status, header, body = response

                    if 'OK' == status:
                        yield header, body
                        seq = header['log_seq'] + 1
