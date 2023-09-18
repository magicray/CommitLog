import os
import sys
import ssl
import json
import uuid
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
                cmd, meta, length = json.loads(req)
            except Exception:
                log(f'{peer} disconnected or invalid header')
                return writer.close()

            if cmd not in HANDLERS:
                log(f'{peer} invalid command {req}')
                return writer.close()

            status, meta, data = HANDLERS[cmd](
                meta, await reader.readexactly(length))

            length = len(data) if data else 0
            res = json.dumps([status, meta, length])

            writer.write(res.encode())
            writer.write(b'\n')
            if length > 0:
                writer.write(data)

            await writer.drain()
            log(f'{peer} {cmd}:{status} {req} {res}')
        except Exception as e:
            traceback.print_exc()
            log(f'{peer} FATAL({e})')
            os._exit(0)


def dump(path, *objects):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp = path + '.' + str(uuid.uuid4()) + '.tmp'
    with open(tmp, 'wb') as fd:
        for obj in objects:
            if type(obj) is not bytes:
                obj = json.dumps(obj, sort_keys=True).encode()

            fd.write(obj)

    os.replace(tmp, path)

    # Force flush all data to disk
    os.sync()


def get_logdir(log_id):
    h = hashlib.sha256(log_id.encode()).hexdigest()
    return os.path.join('logs', h[0:3], h[3:6], log_id)


def get_logfile(log_id, log_seq):
    l1, l2, = log_seq//1000000, log_seq//1000
    return os.path.join(get_logdir(log_id), str(l1), str(l2), str(log_seq))


def max_file(log_id):
    logdir = get_logdir(log_id)

    if not os.path.isdir(logdir):
        return 0, None

    # Get max log_seq for this log_id
    # Traverse the three level directory hierarchy picking the highest
    # numbered dir/file at each level
    l1_dirs = [int(f) for f in os.listdir(logdir) if f.isdigit()]
    for l1 in sorted(l1_dirs, reverse=True):
        l2_dirname = os.path.join(logdir, str(l1))
        l2_dirs = [int(f) for f in os.listdir(l2_dirname) if f.isdigit()]
        for l2 in sorted(l2_dirs, reverse=True):
            l3_dirname = os.path.join(l2_dirname, str(l2))
            files = [int(f) for f in os.listdir(l3_dirname) if f.isdigit()]
            for f in sorted(files, reverse=True):
                return f, os.path.join(l3_dirname, str(f))

    return 0, None


def logseq_server(meta, data):
    return 'OK', max_file(meta)[0], None


def read_server(meta, data):
    what, log_id, log_seq = meta

    path = get_logfile(log_id, log_seq)

    if os.path.isfile(path):
        with open(path, 'rb') as fd:
            meta = json.loads(fd.readline())

            return 'OK', meta, fd.read() if 'data' == what else None

    return 'NOTFOUND', None, None


def paxos_server(meta, data):
    phase = 'promise' if 3 == len(meta) else 'accept'
    log_id, proposal_seq, guid = meta[0], meta[1], meta[2]

    if os.path.dirname(log_id):
        return 'INVALID_LOG_ID', log_id, None

    logdir = get_logdir(log_id)
    promise_filepath = os.path.join(logdir, 'promised')

    promised_seq = 0
    if os.path.isfile(promise_filepath):
        with open(promise_filepath) as fd:
            obj = json.load(fd)
            uuid = obj['uuid']
            promised_seq = obj['promised_seq']

    # Accept this as the new leader as it has higher proposal_seq.
    # Subsequent requests from any stale, older leaders would be rejected
    if proposal_seq > promised_seq:
        uuid = guid
        promised_seq = proposal_seq
        dump(promise_filepath, dict(promised_seq=proposal_seq, uuid=guid))

    # paxos PROMISE phase
    # Return the most recent accepted value.
    # Client will take the most recent of these, across servers and
    # propose that in the ACCEPT phase.
    if 'promise' == phase and proposal_seq == promised_seq and guid == uuid:
        # Most recent file in this log stream
        log_seq, filepath = max_file(log_id)

        # Log stream does not yet exist
        if 0 == log_seq:
            return 'OK', dict(log_seq=0, accepted_seq=0), None

        with open(filepath, 'rb') as fd:
            return 'OK', json.loads(fd.readline()), fd.read()

    # paxos ACCEPT phase
    # Safe to accept as only the most recent leader can reach this stage
    if 'accept' == phase and proposal_seq == promised_seq and guid == uuid:
        log_seq, md5_chain = meta[3], meta[4]

        md5 = hashlib.md5(data).hexdigest()
        hdr = dict(log_id=log_id, log_seq=log_seq, accepted_seq=proposal_seq,
                   md5=md5, md5_chain=md5_chain, uuid=uuid, length=len(data))

        dump(get_logfile(log_id, log_seq), hdr, b'\n', data)

        return 'OK', hdr, None

    return 'STALE_PROPOSAL_SEQ', meta, None


async def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert, port = sys.argv[1], int(sys.argv[2])

    SSL = ssl.create_default_context(
        cafile=cert,
        purpose=ssl.Purpose.CLIENT_AUTH)
    SSL.load_cert_chain(cert, cert)
    SSL.verify_mode = ssl.CERT_REQUIRED

    srv = await asyncio.start_server(server, None, port, ssl=SSL)
    async with srv:
        return await srv.serve_forever()


if '__main__' == __name__:
    asyncio.run(main())
