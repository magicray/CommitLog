import json
import uuid
import time
import commitlog.rpc


class RPCClient(commitlog.rpc.Client):
    def __init__(self, cacert, cert, servers):
        super().__init__(cacert, cert, servers)

    async def filtered(self, resource, octets=b''):
        res = await self.cluster(resource, octets)
        result = dict()

        for s, r in zip(self.conns.keys(), res):
            if r and type(r) in (bytes, str, bool, int, float, list, dict):
                result[s] = r

        return result


class Client():
    def __init__(self, cacert, cert, servers):
        self.client = RPCClient(cacert, cert, servers)
        self.quorum = self.client.quorum
        self.servers = servers

    # PAXOS Client
    async def reset(self):
        self.proposal_seq = self.log_seq = None
        proposal_seq = int(time.strftime('%Y%m%d%H%M%S'))

        # Paxos PROMISE phase - block stale leaders from writing
        url = f'/promise/proposal_seq/{proposal_seq}'
        res = await self.client.filtered(url)
        if self.quorum > len(res):
            raise Exception('NO_QUORUM')

        # CRUX of the paxos protocol - Find the most recent log_seq with most
        # recent accepted_seq. Only this value should be proposed
        srv = commit_id = None
        log_seq = accepted_seq = 0
        for k, v in res.items():
            old = log_seq, accepted_seq
            new = v['log_seq'], v['accepted_seq']

            if new > old:
                srv = k
                log_seq = v['log_seq']
                commit_id = v['commit_id']
                accepted_seq = v['accepted_seq']

        if 0 == log_seq:
            self.log_seq = 0
            self.proposal_seq = proposal_seq
            return dict(log_seq=0)

        # Fetch the octets from the server
        url = f'/read/log_seq/{log_seq}/what/body'
        result = await self.client.server(srv, url)
        if not result:
            raise Exception('LAST_RECORD_NOT_FETCHED')

        hdr, octets = result.split(b'\n', maxsplit=1)
        hdr = json.loads(hdr)

        assert (hdr['length'] == len(octets))
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
        vlist = list((await self.client.filtered(url, octets)).values())

        if self.quorum > len(vlist):
            raise Exception('NO_QUORUM')

        if not all([vlist[0] == v for v in vlist]):
            raise Exception('INCONSISTENT_WRITE')

        self.log_seq = log_seq
        self.proposal_seq = proposal_seq
        return vlist[0]

    async def tail(self, log_seq):
        url = f'/read/log_seq/{log_seq}/what/header'
        res = await self.client.filtered(url)
        if self.quorum > len(res):
            raise Exception('NO_QUORUM')

        hdrs = list()
        for k, v in res.items():
            # accepted_seq, header, server
            hdrs.append((v.pop('accepted_seq'), v, k))

        hdrs = sorted(hdrs, reverse=True)
        url = f'/read/log_seq/{log_seq}/what/body'
        result = await self.client.server(hdrs[0][2], url)

        hdr, octets = result.split(b'\n', maxsplit=1)
        hdr = json.loads(hdr)

        assert (hdr['length'] == len(octets))
        assert (hdr['log_seq'] == hdrs[0][1]['log_seq'])
        assert (hdr['commit_id'] == hdrs[0][1]['commit_id'])
        assert (hdr['accepted_seq'] == hdrs[0][0])

        return hdr, octets

    async def max_seq(self):
        res = await self.client.filtered('/max_seq')
        if self.quorum > len(res):
            raise Exception('NO_QUORUM')

        return max(res.values())

    async def purge(self, log_seq):
        return await self.client.filtered(f'/purge/log_seq/{log_seq}')
