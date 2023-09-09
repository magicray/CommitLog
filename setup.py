import time
from distutils.core import setup

setup(
  name='commitlog',
  modules=['commitlog'],
  version=time.strftime('%Y%m%d'),
  description='Replicated and strongly consistent Commit Log',
  long_description='Paxos for replication and plain filesystem for data. '
                   'Leaderless and highly available.',
  author='Bhupendra Singh',
  author_email='bhsingh@gmail.com',
  url='https://github.com/magicray/CommitLog',
  keywords=['paxos', 'consistent', 'replicated', 'commit', 'log']
)
