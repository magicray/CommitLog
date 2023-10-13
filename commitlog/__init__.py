import os
import re
import ssl
import json
import uuid
import asyncio
import traceback
import urllib.parse
from logging import critical as log


def path_join(*path):
    return os.path.join(*[str(p) for p in path])


def seq2path(logdir, log_seq):
    return path_join(logdir, log_seq//100000, log_seq//1000, log_seq)


def sorted_dir(dirname):
    files = [int(f) for f in os.listdir(dirname) if f.isdigit()]
    return sorted(files, reverse=True)


def max_seq(logdir):
    # Traverse the three level directory hierarchy,
    # picking the highest numbered dir/file at each level
    for x in sorted_dir(logdir):
        for y in sorted_dir(path_join(logdir, x)):
            for f in sorted_dir(path_join(logdir, x, y)):
                return f

    return 0


def dump(path, *objects):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp = path + '.' + str(uuid.uuid4()) + '.tmp'
    with open(tmp, 'wb') as fd:
        for obj in objects:
            if type(obj) is not bytes:
                obj = json.dumps(obj, sort_keys=True).encode()

            fd.write(obj)

    os.replace(tmp, path)


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


class HTTPServer():
    def __init__(self, methods):
        self.methods = methods

    async def handler(self, reader, writer):
        peer = None
        count = 1

        while True:
            try:
                try:
                    peer = writer.get_extra_info('socket').getpeername()

                    line = await reader.readline()
                    p = line.decode().split()[1].strip('/').split('/')

                    method = p[0]
                    params = {k.lower(): urllib.parse.unquote(v)
                              for k, v in zip(p[1::2], p[2::2])}

                    length = 0
                    while True:
                        line = await reader.readline()
                        line = line.strip()
                        if not line:
                            break
                        k, v = line.decode().split(':', maxsplit=1)
                        if 'content-length' == k.strip().lower():
                            length = int(v.strip())

                    if length > 0:
                        params['blob'] = await reader.readexactly(length)
                except Exception:
                    return writer.close()

                if method not in self.methods:
                    return writer.close()

                try:
                    res = await self.methods[method](**params)
                except Exception as e:
                    traceback.print_exc()
                    res = str(e).encode()
                    writer.write(b'HTTP/1.1 400 Bad Request\n')
                    writer.write(f'content-length: {len(res)}\n\n'.encode())
                    writer.write(res)
                    await writer.drain()

                res = res if res else b''
                status = '200 OK' if res else '400 Bad Request'
                mime_type = 'application/octet-stream'
                if type(res) is not bytes:
                    res = json.dumps(res, indent=4, sort_keys=True).encode()
                    mime_type = 'application/json'

                try:
                    writer.write(f'HTTP/1.1 {status}\n'.encode())
                    writer.write(f'content-length: {len(res)}\n'.encode())
                    if res:
                        writer.write(f'content-type: {mime_type}\n\n'.encode())
                        writer.write(res)
                    else:
                        writer.write(b'\n')
                    await writer.drain()
                except Exception:
                    return writer.close()

                params.pop('blob', None)
                log(f'{peer} {count} {method} {params} {length} {len(res)}')
                count += 1
            except Exception as e:
                traceback.print_exc()
                log(f'{peer} {count} FATAL({e})')
                os._exit(0)

    async def run(self, port, cert):
        ctx = cert_context(cert, ssl.Purpose.CLIENT_AUTH)
        srv = await asyncio.start_server(self.handler, None, port, ssl=ctx)

        async with srv:
            return await srv.serve_forever()


class HTTPClient():
    def __init__(self, cert, servers):
        self.SSL = cert_context(cert, ssl.Purpose.SERVER_AUTH)
        self.conns = {tuple(srv): (None, None) for srv in servers}

    async def server(self, server, resource, blob=b''):
        try:
            if self.conns[server][0] is None or self.conns[server][1] is None:
                self.conns[server] = await asyncio.open_connection(
                    server[0], server[1], ssl=self.SSL)

            reader, writer = self.conns[server]

            blob = blob if blob else b''
            if type(blob) is not bytes:
                blob = json.dumps(blob).encode()

            writer.write(f'POST {resource} HTTP/1.1\n'.encode())
            writer.write(f'content-length: {len(blob)}\n\n'.encode())
            writer.write(blob)
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
            if self.conns[server][1] is not None:
                self.conns[server][1].close()
                self.conns[server] = None, None

            raise

    async def cluster(self, resource, blob=b''):
        servers = self.conns.keys()

        res = await asyncio.gather(
            *[self.server(s, resource, blob) for s in servers],
            return_exceptions=True)

        result = dict()
        for s, r in zip(servers, res):
            if type(r) in (bytes, str, int, float, bool, list, dict):
                result[s] = r
        return result

    def __del__(self):
        for server, (reader, writer) in self.conns.items():
            try:
                writer.close()
            except Exception:
                pass


class Client():
    def __init__(self, cert, servers):
        self.client = HTTPClient(cert, servers)
        self.quorum = int(len(servers)/2) + 1
        self.servers = ','.join([f'{ip}:{port}' for ip, port in servers])

    async def init(self):
        self.proposal_seq = self.log_seq = None

        url = f'/init/servers/{self.servers}'
        values = sorted((await self.client.cluster(url)).values())

        if values:
            self.proposal_seq, self.log_seq = values[-1]

        return self.log_seq

    async def write(self, blob):
        proposal_seq, log_seq = self.proposal_seq, self.log_seq + 1
        self.proposal_seq = self.log_seq = None

        url = f'/commit/proposal_seq/{proposal_seq}/log_seq/{log_seq}'
        values = list((await self.client.cluster(url, blob)).values())

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

        header, blob = result.split(b'\n', maxsplit=1)
        hdr = json.loads(header)

        hdr.pop('accepted_seq')
        assert (hdr['length'] == len(blob))
        assert (hdrs[0][1] == json.dumps(hdr, sort_keys=True))

        return hdr, blob
