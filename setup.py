import time
from distutils.core import setup

setup(
  name='commitlog',
  modules=['commitlog'],
  version=time.strftime('%Y%m%d'),
  description='General Purpose Commit Log - '
              'Replicated and Strongly Consistent',
  long_description='Paxos for replication and plain filesystem for data. '
                   'Leaderless and highly available.',
  author='Bhupendra Singh',
  author_email='bhsingh@gmail.com',
  url='https://github.com/magicray/CommitLog',
  keywords=['paxos', 'consistent', 'replicated', 'commit', 'log']
)
