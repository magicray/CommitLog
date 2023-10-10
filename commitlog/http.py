import os
import ssl
import json
import asyncio
import traceback
from logging import critical as log


class Certificate:
    @staticmethod
    def context(path, purpose):
        ctx = ssl.create_default_context(cafile=path, purpose=purpose)

        ctx.load_cert_chain(path, path)
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.check_hostname = False

        return ctx

    @staticmethod
    def subject(ctx):
        return ctx.get_ca_certs()[0]['subject'][0][0][1]


class Server():
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
                    params = {k.lower(): v for k, v in zip(p[1::2], p[2::2])}

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
                    log(f'{peer} {count} disconnected or invalid header')
                    return writer.close()

                if method not in self.methods:
                    log(f'{peer} {count} invalid method {method}')
                    return writer.close()

                res = await self.methods[method](**params)
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
                    log(f'{peer} disconnected or invalid header')
                    return writer.close()

                params.pop('blob', None)
                log(f'{peer} {count} {method} {params} {length} {len(res)}')
                count += 1
            except Exception as e:
                traceback.print_exc()
                log(f'{peer} {count} FATAL({e})')
                os._exit(0)

    async def run(self, port, cert):
        ctx = Certificate.context(cert, ssl.Purpose.CLIENT_AUTH)
        srv = await asyncio.start_server(self.handler, None, port, ssl=ctx)

        async with srv:
            return await srv.serve_forever()


class Client():
    def __init__(self, cert, servers):
        self.SSL = Certificate.context(cert, ssl.Purpose.SERVER_AUTH)
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

            # Ignore the return status as we don't use it
            line = await reader.readline()

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

            if length > 0:
                octets = await reader.readexactly(length)
                if 'application/json' == mime_type:
                    return json.loads(octets)
                return octets
        except Exception as e:
            log(e)
            if self.conns[server][1] is not None:
                self.conns[server][1].close()

            self.conns[server] = None, None

    async def cluster(self, resource, blob=b''):
        servers = self.conns.keys()

        res = await asyncio.gather(
            *[self.server(s, resource, blob) for s in servers],
            return_exceptions=True)

        return {s: r for s, r in zip(servers, res) if r is not None}

    def __del__(self):
        for server, (reader, writer) in self.conns.items():
            try:
                writer.close()
            except Exception:
                pass
