import time
from distutils.core import setup

setup(
  name='CommitLog',
  packages=['commitlog'],
  version=time.strftime('%Y%m%d'),
  description='General Purpose Distributed Commit Log - '
              'Replicated and Strongly Consistent',
  long_description='Leaderless and highly available. '
                   'Multi Paxos for synchronous and consistent replication. '
                   'Plain filesystem for persistence.',
  author='Bhupendra Singh',
  author_email='bhsingh@gmail.com',
  url='https://github.com/magicray/CommitLog',
  keywords=['paxos', 'consistent', 'replicated', 'commit', 'log']
)
