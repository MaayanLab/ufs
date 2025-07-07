import pytest
def ufs():
  from ufs.impl.sync import Sync
  from ufs.impl.client import Client
  from ufs.impl.memory import Memory
  from ufs.impl.logger import Logger
  from ufs.access.server import serve_ufs_via_socket
  ufs = Logger(Memory())
  with serve_ufs_via_socket(ufs) as uri:
    with Sync(Client(uri)) as ufs:
      yield ufs
