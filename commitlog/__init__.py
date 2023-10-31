import json
import uuid
import time
import commitlog.rpc


class Client():
    def __init__(self, cacert, cert, servers):
        self.client = commitlog.rpc.Client(cacert, cert, servers)
        self.quorum = self.client.quorum
        self.servers = servers

        self.log_seq = self.proposal_seq = None

    # PAXOS Client
    async def reset_leader(self, proposal_seq=None, log_seq=None):
        if proposal_seq is not None and log_seq is not None:
            self.log_seq = log_seq
            self.proposal_seq = proposal_seq
            return dict(proposal_seq=self.proposal_seq, log_seq=self.log_seq)

        self.proposal_seq = self.log_seq = None
        proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

        # Paxos PROMISE phase - block stale leaders from writing
        url = f'/promise/proposal_seq/{proposal_seq}'
        res = await self.client.cluster(url)
        if self.quorum > len(res):
            return

        hdrs = set(res.values())
        if 1 == len(hdrs):
            header = hdrs.pop().split(b'\n', maxsplit=1)[0]
            self.log_seq = json.loads(header)['log_seq']
            self.proposal_seq = proposal_seq
            return dict(proposal_seq=self.proposal_seq, log_seq=self.log_seq)

        # CRUX of the paxos protocol - Find the most recent log_seq with most
        # recent accepted_seq. Only this value should be proposed
        log_seq = accepted_seq = 0
        commit_id = str(uuid.uuid4())
        for val in res.values():
            header, body = val.split(b'\n', maxsplit=1)
            header = json.loads(header)

            old = log_seq, accepted_seq
            new = header['log_seq'], header['accepted_seq']

            if new > old:
                octets = body
                log_seq = header['log_seq']
                commit_id = header['commit_id']
                accepted_seq = header['accepted_seq']

        if 0 == log_seq or not octets:
            return

        # Paxos ACCEPT phase - re-write the last blob to sync all the nodes
        url = f'/commit/proposal_seq/{proposal_seq}'
        url += f'/log_seq/{log_seq}/commit_id/{commit_id}'
        vlist = list((await self.client.cluster(url, octets)).values())

        if len(vlist) >= self.quorum and all([vlist[0] == v for v in vlist]):
            self.log_seq = vlist[0]['log_seq']
            self.proposal_seq = proposal_seq
            return dict(proposal_seq=self.proposal_seq, log_seq=self.log_seq)

    async def append(self, octets):
        proposal_seq, log_seq = self.proposal_seq, self.log_seq + 1
        self.proposal_seq = self.log_seq = None

        url = f'/commit/proposal_seq/{proposal_seq}'
        url += f'/log_seq/{log_seq}/commit_id/{uuid.uuid4()}'
        values = list((await self.client.cluster(url, octets)).values())

        if len(values) >= self.quorum:
            if all([values[0] == v for v in values]):
                self.proposal_seq, self.log_seq = proposal_seq, log_seq

                return values[0]

    async def tail(self, log_seq):
        url = f'/read/log_seq/{log_seq}/what/header'
        res = await self.client.cluster(url)
        if self.quorum > len(res):
            return

        hdrs = list()
        for k, v in res.items():
            hdrs.append((v.pop('accepted_seq'),          # accepted seq
                         json.dumps(v, sort_keys=True),  # header
                         k))                             # server

        hdrs = sorted(hdrs, reverse=True)
        if not all([hdrs[0][1] == h[1] for h in hdrs[:self.quorum]]):
            return

        try:
            url = f'/read/log_seq/{log_seq}/what/body'
            result = await self.client.server(hdrs[0][2], url)
            if not result:
                return
        except Exception:
            return

        header, octets = result.split(b'\n', maxsplit=1)
        hdr = json.loads(header)
        hdr.pop('accepted_seq')

        assert (hdr['length'] == len(octets))
        assert (hdrs[0][1] == json.dumps(hdr, sort_keys=True))

        return hdr, octets

    async def purge(self, log_seq):
        return await self.client.cluster(f'/purge/log_seq/{log_seq}')
