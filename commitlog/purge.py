import os
import re
import sys
import ssl
import uuid
import shutil
import logging


def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    cert = sys.argv[1]

    SSL = ssl.create_default_context(
        cafile=cert,
        purpose=ssl.Purpose.CLIENT_AUTH)
    SSL.load_cert_chain(cert, cert)
    SSL.verify_mode = ssl.CERT_REQUIRED

    # log_id = UUID extracted from the certificate subject
    sub = SSL.get_ca_certs()[0]['subject'][0][0][1]
    guid = uuid.UUID(re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', sub)[0])

    trash_dir = os.path.join('commitlog', str(guid), 'tmp')

    for path in os.listdir(trash_dir):
        shutil.rmtree(os.path.join(trash_dir, path))
        logging.critical(f'deleted {path}')


if '__main__' == __name__:
    main()
