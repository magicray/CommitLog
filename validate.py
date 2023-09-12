import os
import sys
import hashlib


def main(log_id):
    log_seq = 0

    h = hashlib.sha256(log_id.encode()).hexdigest()
    logdir = os.path.join('logs', h[0:3], h[3:6], log_id)

    while True:
        l1, l2, = log_seq//1000000, log_seq//1000
        path = os.path.join(logdir, str(l1), str(l2), str(log_seq))

        checksums = dict()
        for i in range(1, 6):
            p = os.path.join(str(i), path)
            if os.path.isfile(p):
                with open(p, 'rb') as fd:
                    md5 = hashlib.md5(fd.read()).hexdigest()
                    checksums.setdefault(md5, 0)
                    checksums[md5] += 1

        if not checksums:
            return

        counts = sorted([(v, k) for k, v in checksums.items()])
        count, md5 = counts[-1][0], counts[-1][1]

        if count < 3:
            return

        print('{} {} {}'.format(log_seq, count, md5))

        log_seq += 1


if '__main__' == __name__:
    main(sys.argv[1])
