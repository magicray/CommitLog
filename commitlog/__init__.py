import ssl
import json
import time
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

        self.conns = {srv: (None, None) for srv in servers}

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


class Client():
    def __init__(self, cert, servers):
        self.rpc = RPC(cert, servers)
        self.quorum = int(len(servers)/2) + 1

        self.log_seq = None
        self.proposal_seq = None

    async def commit(self, blob=None):
        commit_id = str(uuid.uuid4())

        # This client is not a leader yet as log_seq is not set
        if not self.log_seq and not blob:
            # paxos PROMISE phase - block stale leaders from writing
            self.proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

            res = await self.rpc('promise', [self.proposal_seq])
            if self.quorum > len(res):
                raise Exception('NO_QUORUM')

            headers = {json.dumps(h, sort_keys=True) for h, _ in res.values()}

            if 1 == len(headers):
                header = json.loads(headers.pop())
                self.log_seq = header['log_seq'] + 1
                return header

            # Default values if nothing is found in the PROMISE replies
            log_seq, accepted_seq, blob = 0, 0, None

            # This is the CRUX of the paxos protocol
            # Find the most recent log_seq with most recent accepted_seq
            # Only this value should be proposed, else everything breaks
            for header, body in res.values():
                old = log_seq, accepted_seq
                new = header['log_seq'], header['accepted_seq']

                if new > old:
                    blob = body
                    log_seq = header['log_seq']
                    commit_id = header['commit_id']
                    accepted_seq = header['accepted_seq']

        if not blob:
            raise Exception(f'EMPTY_BLOB log_seq({log_seq})')

        if self.log_seq:
            # Remove leadership. Reinstate if the next write is successful
            log_seq, self.log_seq = self.log_seq, None

        # paxos ACCEPT phase - write a new blob
        # Ignore tmep failure and retry a few times
        for delay in (1, 1, 1, 1, 0):
            header = [self.proposal_seq, log_seq, commit_id]
            res = await self.rpc('accept', header, blob)

            if self.quorum > len(res):
                await asyncio.sleep(delay)
                continue

            headers = {json.dumps(h, sort_keys=True) for h, _ in res.values()}

            # Write successful. Reinstate as the leader.
            if 1 == len(headers):
                header = json.loads(headers.pop())
                self.log_seq = header['log_seq'] + 1
                return header

        raise Exception(f'NO_QUORUM log_seq({log_seq})')

    async def tail(self, seq, wait_sec=1):
        max_seq = seq - 1

        while True:
            if seq > max_seq:
                res = await self.rpc('logseq')
                max_seq = max([v[0] for v in res.values()])

            if seq > max_seq:
                await asyncio.sleep(wait_sec)
                continue

            res = await self.rpc('read', ['header', seq])
            if self.quorum > len(res):
                await asyncio.sleep(wait_sec)
                continue

            hdrs = list()
            for k, v in res.items():
                hdrs.append((
                    v[0].pop('accepted_seq'),          # accepted seq
                    json.dumps(v[0], sort_keys=True),  # record metadata
                    k))                                # server

            hdrs = sorted(hdrs, reverse=True)
            latest = hdrs[0][1]
            all_ok = True
            for i in range(self.quorum):
                if latest != hdrs[i][1]:
                    all_ok = False
                    log('NOT_YET_FINALIZED', json.dumps(hdrs, indent=4))

            if all_ok is not True:
                await asyncio.sleep(wait_sec)
                continue

            result = await self.rpc.rpc(hdrs[0][2], 'read', ['body', seq])
            if not result or 'OK' != result[0]:
                await asyncio.sleep(wait_sec)
                continue

            result[1].pop('accepted_seq')
            assert (hdrs[0][1] == json.dumps(result[1], sort_keys=True))

            yield result[1], result[2]
            seq = seq + 1
