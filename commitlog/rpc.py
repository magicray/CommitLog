import os
import ssl
import json
import asyncio
import traceback
from logging import critical as log


# Convert async coroutines an async_generator.
def async_generator(f):
    async def wrapper(*args, **kwarg):
        yield await f(*args, **kwarg)

    return wrapper


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


class Handler():
    def __init__(self, methods):
        self.methods = methods

    async def __call__(self, reader, writer):
        peer = None

        while True:
            try:
                try:
                    peer = writer.get_extra_info('socket').getpeername()

                    req = await reader.readline()
                    if not req or len(req) > 1024:
                        if req:
                            log(f'{peer} header too long {len(req)} > 1KB')
                        return writer.close()

                    req = req.decode().strip()
                    method, header, length = json.loads(req)
                except Exception:
                    log(f'{peer} disconnected or invalid header')
                    return writer.close()

                if method not in self.methods or length > 10*1024*1024:
                    log(f'{peer} invalid request {req}')
                    return writer.close()

                handler = self.methods[method]
                itr = handler(header, await reader.readexactly(length))

                async for status, header, body in itr:
                    length = len(body) if body else 0
                    res = json.dumps([status, header, length])

                    try:
                        writer.write(res.encode())
                        writer.write(b'\n')
                        if length > 0:
                            writer.write(body)
                        await writer.drain()
                    except Exception:
                        log(f'{peer} disconnected or invalid header')
                        return writer.close()

                    log(f'{peer} {method}:{status} {req} {res}')
            except Exception as e:
                traceback.print_exc()
                log(f'{peer} FATAL({e})')
                os._exit(0)


async def server(port, cert, methods):
    ctx = Certificate.context(cert, ssl.Purpose.CLIENT_AUTH)
    srv = await asyncio.start_server(Handler(methods), None, port, ssl=ctx)

    async with srv:
        return await srv.serve_forever()


class Client():
    def __init__(self, cert, servers):
        self.SSL = Certificate.context(cert, ssl.Purpose.SERVER_AUTH)
        self.conns = {tuple(srv): (None, None) for srv in servers}

    async def stream(self, server, method, header):
        try:
            reader, writer = await asyncio.open_connection(
                server[0], server[1], ssl=self.SSL)

            writer.write(json.dumps([method, header, 0]).encode())
            writer.write(b'\n')
            await writer.drain()

            while True:
                status, header, length = json.loads(await reader.readline())
                yield status, header, await reader.readexactly(length)
        except Exception as e:
            log(e)

    async def server(self, server, method, header=None, body=b''):
        try:
            if self.conns[server][0] is None or self.conns[server][1] is None:
                self.conns[server] = await asyncio.open_connection(
                    server[0], server[1], ssl=self.SSL)

            reader, writer = self.conns[server]

            if body and type(body) is not bytes:
                body = json.dumps(body).encode()

            length = len(body) if body else 0
            header = json.dumps([method, header, length])

            writer.write(header.encode())
            writer.write(b'\n')
            if length > 0:
                writer.write(body)
            await writer.drain()

            header = await reader.readline()
            assert (header), 'EMPTY_HEADER'

            status, header, length = json.loads(header)
            body = await reader.readexactly(length)
            assert (length == len(body)), f'INVALID_LEN {length} {len(body)}'

            return status, header, body
        except Exception as e:
            log(e)
            if self.conns[server][1] is not None:
                self.conns[server][1].close()

            self.conns[server] = None, None

    async def cluster(self, method, header=None, body=b''):
        servers = self.conns.keys()

        res = await asyncio.gather(
            *[self.server(s, method, header, body) for s in servers],
            return_exceptions=True)

        return {s: r for s, r in zip(servers, res) if type(r) is tuple}

    async def __call__(self, method, header=None, body=b''):
        result = await self.cluster(method, header, body)
        return {k: (v[1], v[2]) for k, v in result.items() if 'OK' == v[0]}

    def __del__(self):
        for server, (reader, writer) in self.conns.items():
            if writer is not None:
                try:
                    writer.close()
                except Exception as e:
                    log(e)
