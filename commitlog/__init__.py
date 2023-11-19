import ssl
import uuid
import json
import time
import hashlib
import commitlog.rpc


class RPCClient(commitlog.rpc.Client):
    def __init__(self, cacert, cert, servers):
        super().__init__(cacert, cert, servers)

    async def filtered(self, resource, octets=b''):
        res = await self.cluster(resource, octets)
        result = dict()

        for s, r in zip(self.conns.keys(), res):
            if r and type(r) is bytes:
                result[s] = r

        return result


class Client():
    def __init__(self, cacert, cert, servers):
        self.client = RPCClient(cacert, cert, servers)
        self.quorum = self.client.quorum
        self.servers = servers

        cert = ssl.create_default_context(cafile=cert).get_ca_certs()[0]
        self.cert_subject = str(uuid.UUID(cert['subject'][0][0][1]))

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
        hdr = dict(log_seq=-1, accepted_seq=-1)
        octets = None
        for v in res.values():
            new_hdr, new_octets = v.split(b'\n', maxsplit=1)
            new_hdr = json.loads(new_hdr)

            old = hdr['log_seq'], hdr['accepted_seq']
            new = new_hdr['log_seq'], new_hdr['accepted_seq']

            if new > old:
                hdr = new_hdr
                octets = new_octets

        if 0 == hdr['log_seq']:
            self.log_seq = 0
            self.proposal_seq = proposal_seq
            return dict(log_seq=0)

        assert (hdr['length'] == len(octets))
        assert (hdr['log_id'] == self.cert_subject)
        assert (hdr['log_seq'] > -1)
        assert (hdr['checksum'] == hashlib.md5(octets).hexdigest())

        # Paxos ACCEPT phase - re-write the last blob to sync all the nodes
        self.log_seq = hdr['log_seq'] - 1
        self.proposal_seq = proposal_seq
        return await self.append(octets)

    async def append(self, octets):
        proposal_seq, log_seq = self.proposal_seq, self.log_seq + 1
        self.proposal_seq = self.log_seq = None

        checksum = hashlib.md5(octets).hexdigest()

        url = f'/commit/proposal_seq/{proposal_seq}'
        url += f'/log_seq/{log_seq}/checksum/{checksum}'
        res = await self.client.filtered(url, octets)
        if self.quorum > len(res):
            raise Exception('NO_QUORUM')

        vset = set(res.values())
        if 1 != len(vset):
            raise Exception('INCONSISTENT_WRITE')

        hdr = json.loads(vset.pop())
        assert (hdr['length'] == len(octets))
        assert (hdr['log_id'] == self.cert_subject)
        assert (hdr['log_seq'] == log_seq)
        assert (hdr['checksum'] == checksum)
        assert (hdr['accepted_seq'] == proposal_seq)

        self.log_seq = log_seq
        self.proposal_seq = proposal_seq
        return hdr

    async def tail(self, log_seq):
        url = f'/read/log_seq/{log_seq}/length/512'
        res = await self.client.filtered(url)
        if self.quorum > len(res):
            raise Exception('NO_QUORUM')

        srv = None
        header = dict(accepted_seq=0)
        for k, v in res.items():
            v = json.loads(v.split(b'\n')[0])

            if v['accepted_seq'] > header['accepted_seq']:
                srv, header = k, v

        res = await self.client.server(srv, f'/read/log_seq/{log_seq}')
        hdr, octets = res.split(b'\n', maxsplit=1)
        hdr = json.loads(hdr)

        assert (hdr['length'] == len(octets))
        assert (hdr['log_id'] == self.cert_subject)
        assert (hdr['log_seq'] == log_seq)
        assert (hdr['checksum'] == hashlib.md5(octets).hexdigest())
        assert (hdr['accepted_seq'] == header['accepted_seq'])

        return hdr, octets

    async def max_seq(self):
        res = await self.client.filtered('/max_seq')
        if self.quorum > len(res):
            raise Exception('NO_QUORUM')

        return max([json.loads(v) for v in res.values()])

    async def purge(self, log_seq):
        return await self.client.filtered(f'/purge/log_seq/{log_seq}')
