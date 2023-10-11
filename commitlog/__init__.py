import os
import json
import uuid


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


def dump(logdir, path, *objects):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    tmp = os.path.join(logdir, str(uuid.uuid4()) + '.tmp')
    with open(tmp, 'wb') as fd:
        for obj in objects:
            if type(obj) is not bytes:
                obj = json.dumps(obj, sort_keys=True).encode()

            fd.write(obj)

    os.replace(tmp, path)
