import ssl
import json
import time
import uuid
import hashlib
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

    async def rpc(self, server, cmd, meta=None, data=b''):
        try:
            if self.conns[server][0] is None or self.conns[server][1] is None:
                self.conns[server] = await asyncio.open_connection(
                    server[0], server[1], ssl=self.SSL)

            reader, writer = self.conns[server]

            if data and type(data) is not bytes:
                data = json.dumps(data).encode()

            length = len(data) if data else 0

            writer.write(json.dumps([cmd, meta, length]).encode())
            writer.write(b'\n')
            if length > 0:
                writer.write(data)
            await writer.drain()

            status, meta, length = json.loads(await reader.readline())

            return status, meta, await reader.readexactly(length)
        except Exception as e:
            log(e)
            if self.conns[server][1] is not None:
                self.conns[server][1].close()

            self.conns[server] = None, None, None

    async def __call__(self, cmd, meta=None, data=b''):
        servers = self.conns.keys()

        res = await asyncio.gather(
            *[self.rpc(s, cmd, meta, data) for s in servers],
            return_exceptions=True)

        return {s: (r[1], r[2]) for s, r in zip(servers, res)
                if type(r) is tuple and 'OK' == r[0]}


class Client():
    def __init__(self, cert, servers):
        self.rpc = RPC(cert, servers)
        self.quorum = int(len(servers)/2) + 1
        self.leader = None

    async def commit(self, blob=None):
        commit_id = str(uuid.uuid4())

        if self.leader is None and not blob:
            # paxos PROMISE phase - block stale leaders from writing
            proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

            res = await self.rpc('promise', [proposal_seq])
            if self.quorum > len(res):
                raise Exception('NO_QUORUM')

            meta_set = set()
            for meta, data in res.values():
                meta_set.add(json.dumps(meta, sort_keys=True))

            if 1 == len(meta_set) and 0 != meta['log_seq']:
                # Last log was written successfully to a majority
                meta = json.loads(meta_set.pop())
                meta.pop('accepted_seq')
                m = json.dumps(meta, sort_keys=True).encode()
                md5_chain = hashlib.md5(m).hexdigest()
                log_seq = meta['log_seq']

                self.leader = [proposal_seq, log_seq+1, md5_chain]
                return meta

            # Default values if nothing is found in the PROMISE replies
            blob = commit_id.encode()
            md5_chain = commit_id
            log_seq, accepted_seq = 0, 0

            # This is the CRUX of the paxos protocol
            # Find the most recent log_seq with most recent accepted_seq
            # Only this value should be proposed, else everything breaks
            for meta, data in res.values():
                old = log_seq, accepted_seq
                new = meta['log_seq'], meta['accepted_seq']

                if new > old:
                    blob = data
                    commit_id = meta['commit_id']
                    md5_chain = meta['md5_chain']

                    log_seq = meta['log_seq']
                    accepted_seq = meta['accepted_seq']

        if not blob:
            raise Exception(f'EMPTY_BLOB log_seq({log_seq})')

        if self.leader:
            # Take away leadership temporarily.
            proposal_seq, log_seq, md5_chain = self.leader
            self.leader = None

        # paxos ACCEPT phase - write a new blob
        # Retry a few times to overcome temp failures
        for delay in (1, 1, 1, 1, 1, 0):
            meta = [proposal_seq, log_seq, commit_id, md5_chain]
            res = await self.rpc('accept', meta, blob)

            if self.quorum > len(res):
                await asyncio.sleep(delay)
                continue

            meta_set = set([json.dumps(meta, sort_keys=True)
                            for meta, data in res.values()])

            # Write successful. Reinstate as the leader.
            if 1 == len(meta_set):
                meta = json.loads(meta_set.pop())
                meta.pop('accepted_seq')
                m = json.dumps(meta, sort_keys=True).encode()
                md5_chain = hashlib.md5(m).hexdigest()

                self.leader = [proposal_seq, log_seq+1, md5_chain]
                return meta

        raise Exception(f'NO_QUORUM log_seq({log_seq})')

    async def tail(self, seq, wait_sec=1):
        max_seq = seq
        md5_chain = None

        while True:
            res = await self.rpc('logseq')
            if self.quorum > len(res):
                await asyncio.sleep(wait_sec)
                continue

            max_seq = max([v[0] for v in res.values()])
            if seq >= max_seq:
                return

            while seq < max_seq:
                res = await self.rpc('read', ['meta', seq])
                if self.quorum > len(res):
                    await asyncio.sleep(wait_sec)
                    continue

                srv = None
                accepted_seq = 0
                for server, res in res.items():
                    if res[0]['accepted_seq'] > accepted_seq:
                        srv = server
                        accepted_seq = res[0]['accepted_seq']

                if not srv:
                    await asyncio.sleep(wait_sec)
                    continue

                result = await self.rpc.rpc(srv, 'read', ['data', seq])
                if not result or 'OK' != result[0]:
                    await asyncio.sleep(wait_sec)
                    continue

                status, meta, data = result
                if md5_chain and md5_chain != meta['md5_chain']:
                    raise Exception(f'MD5_CHAIN_MISMATCH {seq} {md5_chain}')

                meta.pop('accepted_seq')
                m = json.dumps(meta, sort_keys=True).encode()
                md5_chain = hashlib.md5(m).hexdigest()

                yield meta, data
                seq = seq + 1
