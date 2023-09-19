import os
import sys
import uuid


def main():
    guid = str(uuid.uuid4()) if 1 == len(sys.argv) else uuid.UUID(sys.argv[1])

    cmd = 'openssl req -x509 -newkey rsa:4096 -nodes -sha256 -days 4000 '
    cmd += f'-subj "/CN=CommitLog - {guid}" -keyout {guid}.pem -out {guid}.pem'
    os.system(cmd)

    os.system(f'openssl x509 -in {guid}.pem -text -noout')


if '__main__' ==  __name__:
    main()
