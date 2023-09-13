import os
import sys
import json
import hashlib


def main(log_id, log_seq):
    h = hashlib.sha256(log_id.encode()).hexdigest()
    logdir = os.path.join('logs', h[0:3], h[3:6], log_id)

    while True:
        l1, l2, = log_seq//1000000, log_seq//1000
        path = os.path.join(logdir, str(l1), str(l2), str(log_seq))

        headers = list()
        for i in range(1, 6):
            p = os.path.join(str(i), path)
            if os.path.isfile(p):
                with open(p, 'rb') as fd:
                    headers.append(json.loads(fd.readline()))

        if not headers:
            return

        accepted_seq = max([h['accepted_seq'] for h in headers])
        valid = [h for h in headers if h['accepted_seq'] == accepted_seq]

        if len(valid) < 3:
            return

        first = valid[0]
        for v in valid:
            for k in first:
                if v[k] != first[k]:
                    return

        print((len(valid), first))

        log_seq += 1


if '__main__' == __name__:
    main(sys.argv[1], int(sys.argv[2]))
