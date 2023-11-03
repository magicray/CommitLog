import json
import uuid
import time
import commitlog.rpc


class Client():
    def __init__(self, cacert, cert, servers):
        self.client = commitlog.rpc.Client(cacert, cert, servers)
        self.quorum = self.client.quorum
        self.servers = servers

    def init(self, proposal_seq, log_seq):
        self.log_seq = log_seq
        self.proposal_seq = proposal_seq

    # PAXOS Client
    async def reset(self):
        self.proposal_seq = self.log_seq = None
        proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

        # Paxos PROMISE phase - block stale leaders from writing
        url = f'/promise/proposal_seq/{proposal_seq}'
        res = await self.client.cluster(url)
        if self.quorum > len(res):
            return

        vals = set([json.dumps(v, sort_keys=True) for v in res.values()])
        if 1 == len(vals):
            self.log_seq = json.loads(vals.pop())['log_seq']
            self.proposal_seq = proposal_seq
            return dict(proposal_seq=self.proposal_seq, log_seq=self.log_seq)

        # CRUX of the paxos protocol - Find the most recent log_seq with most
        # recent accepted_seq. Only this value should be proposed
        srv = None
        log_seq = accepted_seq = 0
        commit_id = str(uuid.uuid4())
        for k, v in res.items():
            old = log_seq, accepted_seq
            new = v['log_seq'], v['accepted_seq']

            if new > old:
                srv = k
                log_seq = v['log_seq']
                commit_id = v['commit_id']
                accepted_seq = v['accepted_seq']

        if 0 == log_seq:
            return

        # Found the location of the most recent log record
        # NowfFetch the octets from the server
        url = f'/read/log_seq/{log_seq}/what/body'
        result = await self.client.server(srv, url)
        if not result:
            return

        hdr, octets = result.split(b'\n', maxsplit=1)
        hdr = json.loads(hdr)

        assert (hdr['length'] == len(octets))
        assert (hdr['log_seq'] == log_seq)
        assert (hdr['commit_id'] == commit_id)
        assert (hdr['accepted_seq'] == accepted_seq)

        # Paxos ACCEPT phase - re-write the last blob to sync all the nodes
        self.log_seq = log_seq - 1
        self.proposal_seq = proposal_seq
        return await self.append(octets, commit_id)

    async def append(self, octets, commit_id=None):
        proposal_seq, log_seq = self.proposal_seq, self.log_seq + 1
        self.proposal_seq = self.log_seq = None

        commit_id = commit_id if commit_id else str(uuid.uuid4())

        url = f'/commit/proposal_seq/{proposal_seq}'
        url += f'/log_seq/{log_seq}/commit_id/{commit_id}'
        vlist = list((await self.client.cluster(url, octets)).values())

        if len(vlist) >= self.quorum and all([vlist[0] == v for v in vlist]):
            self.proposal_seq, self.log_seq = proposal_seq, log_seq
            return vlist[0]

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
