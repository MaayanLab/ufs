def ufs():
  from ufs.impl.writecache import Writecache
  from ufs.impl.memory import Memory
  from ufs.impl.tempdir import TemporaryDirectory
  with Writecache(TemporaryDirectory(), Memory()) as ufs:
    yield ufs
