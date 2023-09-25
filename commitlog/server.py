import os
import re
import sys
import ssl
import json
import uuid
import shutil
import hashlib
import asyncio
import logging
import traceback
from logging import critical as log


async def server(reader, writer):
    HANDLERS = dict(promise=paxos_server, accept=paxos_server,
                    logseq=logseq_server, read=read_server)

    peer = writer.get_extra_info('socket').getpeername()

    while True:
        try:
            try:
                req = await reader.readline()
                if not req:
                    return writer.close()

                req = req.decode().strip()
                method, header, length = json.loads(req)
            except Exception:
                log(f'{peer} disconnected or invalid header')
                return writer.close()

            if method not in HANDLERS:
                log(f'{peer} invalid command {req}')
                return writer.close()

            status, header, body = HANDLERS[method](
                header, await reader.readexactly(length))

            length = len(body) if body else 0
            res = json.dumps([status, header, length])

            writer.write(res.encode())
            writer.write(b'\n')
            if length > 0:
                writer.write(body)

            await writer.drain()
            log(f'{peer} {method}:{status} {req} {res}')
        except Exception as e:
            traceback.print_exc()
            log(f'{peer} FATAL({e})')
            os._exit(0)


def path_join(*path):
    return os.path.join(*[str(p) for p in path])


def seq2path(log_seq):
    return path_join(G.logdir, log_seq//100000, log_seq//1000, log_seq)


def listdir(dirname):
    files = [int(f) for f in os.listdir(dirname) if f.isdigit()]
    return sorted(files, reverse=True)


def latest_logseq():
    # Traverse the three level directory hierarchy,
    # picking the highest numbered dir/file at each level
    for x in listdir(G.logdir):
        for y in listdir(path_join(G.logdir, x)):
            for f in listdir(path_join(G.logdir, x, y)):
                return f

    return 0


def logseq_server(header, body):
    return 'OK', latest_logseq(), None


def read_server(header, body):
    what, log_seq = header

    path = seq2path(log_seq)
    if not os.path.isfile(path):
        return 'NOTFOUND', None, None

    with open(path, 'rb') as fd:
        header = json.loads(fd.readline())

        return 'OK', header, fd.read() if 'body' == what else None


def dump(path, *objects):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp = os.path.join(G.logdir, str(uuid.uuid4()) + '.tmp')
    with open(tmp, 'wb') as fd:
        for obj in objects:
            if type(obj) is not bytes:
                obj = json.dumps(obj, sort_keys=True).encode()

            fd.write(obj)

    os.replace(tmp, path)
    os.sync()


def paxos_server(header, body):
    phase = 'promise' if 1 == len(header) else 'accept'
    proposal_seq = header[0]

    promised_seq = 0
    promise_filepath = os.path.join(G.logdir, 'promised')
    if os.path.isfile(promise_filepath):
        with open(promise_filepath) as fd:
            promised_seq = json.load(fd)['promised_seq']

    # PROMISE - Block stale leaders and return the most recent accepted value.
    # Client will propose the most recent across servers in the accept phase
    if 'promise' == phase and proposal_seq > promised_seq:
        dump(promise_filepath, dict(promised_seq=proposal_seq))

        # Most recent log file
        log_seq = latest_logseq()

        # Log stream does not yet exist
        if 0 == log_seq:
            return 'OK', dict(log_seq=0, accepted_seq=0), None

        with open(seq2path(log_seq), 'rb') as fd:
            return 'OK', json.loads(fd.readline()), fd.read()

    # ACCEPT - Client has sent the most recent value from the promise phase.
    # Stale leaders blocked. Only the most recent can reach this stage.
    if 'accept' == phase and proposal_seq >= promised_seq:
        log_seq, commit_id = header[1], header[2]

        # Continuous cleanup - remove an older file before writing a new one
        # Retain only the most recent 1 million log records
        old_log_record = seq2path(log_seq - 1000*1000)
        if os.path.isfile(old_log_record):
            os.remove(old_log_record)
            log(f'removed old record log_seq({old_log_record})')

        if proposal_seq > promised_seq:
            dump(promise_filepath, dict(promised_seq=proposal_seq))

        header = dict(accepted_seq=proposal_seq, log_id=G.log_id,
                      log_seq=log_seq, commit_id=commit_id,
                      length=len(body), sha1=hashlib.sha1(body).hexdigest())

        dump(seq2path(header['log_seq']), header, b'\n', body)

        return 'OK', header, None

    return 'STALE_PROPOSAL_SEQ', None, None


class G:
    log_id = None
    logdir = None


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert, port = sys.argv[1], int(sys.argv[2])

    SSL = ssl.create_default_context(
        cafile=cert,
        purpose=ssl.Purpose.CLIENT_AUTH)
    SSL.load_cert_chain(cert, cert)
    SSL.verify_mode = ssl.CERT_REQUIRED

    # Extract UUID from the subject. This would be the log_id for this stream
    sub = SSL.get_ca_certs()[0]['subject'][0][0][1]
    guid = uuid.UUID(re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', sub)[0])

    G.log_id = str(guid)
    G.logdir = os.path.join('commitlog', G.log_id)
    os.makedirs(G.logdir, exist_ok=True)

    data_dirs = listdir(G.logdir)

    # Cleanup
    for f in os.listdir(G.logdir):
        if 'promised' == f:
            continue

        path = os.path.join(G.logdir, str(f))

        if f.isdigit():
            if int(f) < data_dirs[0] - 10:
                shutil.rmtree(path)
        else:
            if os.path.isfile(path):
                os.remove(path)
            else:
                shutil.rmtree(path)

    srv = await asyncio.start_server(server, None, port, ssl=SSL)
    async with srv:
        return await srv.serve_forever()


if '__main__' == __name__:
    asyncio.run(main())
