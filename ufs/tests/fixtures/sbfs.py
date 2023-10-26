import pytest

def ufs():
  import os
  import uuid
  from ufs.impl.sync import Sync
  from ufs.impl.sbfs import SBFS
  from ufs.impl.prefix import Prefix
  from ufs.impl.memory import Memory
  from ufs.impl.writecache import Writecache
  from ufs.utils.pathlib import SafePurePosixPath
  from ufs.access.shutil import rmtree
  try:
    with Writecache(
      Prefix(
        Sync(SBFS(os.environ['SBFS_AUTH_TOKEN'])),
        SafePurePosixPath(os.environ['SBFS_PREFIX'])/str(uuid.uuid4())
      ),
      Memory()
    ) as ufs:
      ufs.mkdir('/')
      try:
        yield ufs
      finally:
        rmtree(ufs, '/')
  except KeyError:
    pytest.skip('Environment variables SBFS_AUTH_TOKEN and SBFS_PREFIX required for sbfs')
