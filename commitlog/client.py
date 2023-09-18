import ssl
import json
import time
import uuid
import hashlib
import asyncio
import traceback
from logging import critical as log


class RPC():
    def __init__(self, cert):
        self.SSL = ssl.create_default_context(
            cafile=cert,
            purpose=ssl.Purpose.SERVER_AUTH)
        self.SSL.load_cert_chain(cert, cert)
        self.SSL.verify_mode = ssl.CERT_REQUIRED

        # Extract server ip:port from subjectAltName
        cert = self.SSL.get_ca_certs()[0]
        servers = [y for x, y in cert['subjectAltName'] if ':' in y]
        servers = [h.split(':') for h in servers]
        servers = set([(ip, int(port)) for ip, port in servers])

        self.conns = {srv: (None, None) for srv in servers}

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


class Client():
    def __init__(self, cert):
        self.rpc = RPC(cert)
        self.logs = dict()
        self.quorum = int(len(self.rpc.conns)/2) + 1

    async def commit(self, log_id, blob=None):
        if log_id not in self.logs and not blob:
            # paxos PROMISE phase - block stale leaders from writing
            guid = str(uuid.uuid4())
            proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

            res = await self.rpc('promise', [log_id, proposal_seq, guid])
            if self.quorum > len(res):
                raise Exception(f'NO_QUORUM log_id({log_id})')

            meta_set = set()
            for meta, data in res.values():
                meta_set.add(json.dumps(meta, sort_keys=True))

            if 1 == len(meta_set) and 0 != meta['log_seq']:
                # Last log was written successfully to a majority
                meta = meta_set.pop()
                md5 = hashlib.md5(meta.encode()).hexdigest()

                meta = json.loads(meta)
                log_seq = meta['log_seq']

                self.logs[log_id] = [proposal_seq, guid, log_seq+1, md5]
                return meta

            # Default values if nothing is found in the PROMISE replies
            md5 = str(uuid.uuid4())
            blob = md5.encode()
            log_seq = accepted_seq = 0

            # This is the CRUX of the paxos protocol
            # Find the most recent log_seq with most recent accepted_seq
            # Only this value should be proposed, else everything breaks
            for meta, data in res.values():
                old = log_seq, accepted_seq
                new = meta['log_seq'], meta['accepted_seq']

                if new > old:
                    md5 = meta['md5_chain']
                    blob = data

                    log_seq = meta['log_seq']
                    accepted_seq = meta['accepted_seq']

        if not blob:
            raise Exception(f'EMPTY_BLOB log_id({log_id}) log_seq({log_seq})')

        if log_id in self.logs:
            # Take away leadership temporarily.
            proposal_seq, guid, log_seq, md5 = self.logs.pop(log_id)

        # paxos ACCEPT phase - write a new blob
        # Retry a few times to overcome temp failures
        for delay in (1, 1, 1, 1, 1, 0):
            meta = [log_id, proposal_seq, guid, log_seq, md5]
            res = await self.rpc('accept', meta, blob)

            if self.quorum > len(res):
                await asyncio.sleep(delay)
                continue

            meta = set([json.dumps(meta, sort_keys=True)
                        for meta, data in res.values()])

            # Write successful. Reinstate as the leader.
            if 1 == len(meta):
                meta = meta.pop()
                md5 = hashlib.md5(meta.encode()).hexdigest()
                self.logs[log_id] = [proposal_seq, guid, log_seq+1, md5]

                return json.loads(meta)

        raise Exception(f'NO_QUORUM log_id({log_id}) log_seq({log_seq})')

    async def tail(self, log_id, seq, wait_sec=1):
        max_seq = seq
        md5_chain = None

        while True:
            res = await self.rpc('logseq', log_id)
            if self.quorum > len(res):
                await asyncio.sleep(wait_sec)

            max_seq = max([v[0] for v in res.values()])
            if seq >= max_seq:
                return

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
                    await asyncio.sleep(wait_sec)
                    continue

                meta, data = res[srv]
                if md5_chain and md5_chain != meta['md5_chain']:
                    raise Exception(f'MD5_CHAIN_MISMATCH {md5_chain}')
                    return

                hdr = json.dumps(meta, sort_keys=True).encode()
                md5_chain = hashlib.md5(hdr).hexdigest()

                yield meta, data
                seq = seq + 1
