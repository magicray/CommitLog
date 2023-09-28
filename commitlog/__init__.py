import json
import uuid
import asyncio
import commitlog.rpc


class Client():
    def __init__(self, cert, servers):
        self.rpc = commitlog.rpc.Client(cert, servers)
        self.quorum = int(len(servers)/2) + 1
        self.servers = servers

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

                await asyncio.sleep(1)
