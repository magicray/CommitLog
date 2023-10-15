import re
import ssl
import json
import uuid
import time
import asyncio
import traceback


def cert_context(path, purpose):
    ctx = ssl.create_default_context(cafile=path, purpose=purpose)

    ctx.load_cert_chain(path, path)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = False

    return ctx


def cert_uuid(cert):
    ctx = cert_context(cert, ssl.Purpose.CLIENT_AUTH)
    return str(uuid.UUID(re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}',
                         ctx.get_ca_certs()[0]['subject'][0][0][1])[0]))


class HTTPClient():
    def __init__(self, cert, servers):
        servers = [s.split(':') for s in servers.split(',')]

        self.SSL = cert_context(cert, ssl.Purpose.SERVER_AUTH)
        self.conns = {(ip, int(port)): (None, None) for ip, port in servers}
        self.quorum = int(len(self.conns)/2) + 1

    async def server(self, server, resource, octets=b''):
        status = None

        try:
            if self.conns[server][0] is None or self.conns[server][1] is None:
                self.conns[server] = await asyncio.open_connection(
                    server[0], server[1], ssl=self.SSL)

            reader, writer = self.conns[server]

            octets = octets if octets else b''
            if type(octets) is not bytes:
                octets = json.dumps(octets).encode()

            writer.write(f'POST {resource} HTTP/1.1\n'.encode())
            writer.write(f'content-length: {len(octets)}\n\n'.encode())
            writer.write(octets)
            await writer.drain()

            status = await reader.readline()

            length = 0
            while True:
                line = await reader.readline()
                line = line.strip()
                if not line:
                    break
                k, v = line.decode().split(':', maxsplit=1)
                if 'content-length' == k.strip().lower():
                    length = int(v.strip())
                if 'content-type' == k.strip().lower():
                    mime_type = v.strip()

            if status.startswith(b'HTTP/1.1 200 OK') and length > 0:
                octets = await reader.readexactly(length)
                assert (length == len(octets))
                if 'application/json' == mime_type:
                    return json.loads(octets)
                return octets
        except Exception:
            if status:
                traceback.print_exc()

            if self.conns[server][1] is not None:
                self.conns[server][1].close()
                self.conns[server] = None, None

    async def cluster(self, resource, octets=b''):
        servers = self.conns.keys()

        res = await asyncio.gather(
            *[self.server(s, resource, octets) for s in servers])

        return {s: r for s, r in zip(servers, res) if r}

    def __del__(self):
        for server, (reader, writer) in self.conns.items():
            try:
                writer.close()
            except Exception:
                pass


class Client():
    def __init__(self, cert, servers):
        self.client = HTTPClient(cert, servers)
        self.quorum = self.client.quorum
        self.servers = servers

    # PAXOS Client
    async def init(self):
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
            return self.log_seq

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
            return self.log_seq

    async def write(self, octets):
        proposal_seq, log_seq = self.proposal_seq, self.log_seq + 1
        self.proposal_seq = self.log_seq = None

        url = f'/commit/proposal_seq/{proposal_seq}'
        url += f'/log_seq/{log_seq}/commit_id/{uuid.uuid4()}'
        values = list((await self.client.cluster(url, octets)).values())

        if len(values) >= self.quorum:
            if all([values[0] == v for v in values]):
                self.proposal_seq, self.log_seq = proposal_seq, log_seq

                return values[0]

    async def read(self, seq):
        url = f'/fetch/log_seq/{seq}/what/header'
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
            url = f'/fetch/log_seq/{seq}/what/body'
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
