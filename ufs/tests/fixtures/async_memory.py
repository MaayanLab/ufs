def ufs():
  from ufs.impl.sync import Sync
  from ufs.impl.prefix import Prefix
  from ufs.impl.memory import Memory
  from ufs.impl.simpleasyn import SimpleAsync
  from ufs.utils.pathlib import SafePurePosixPath
  with Prefix(Sync(SimpleAsync(Memory())), '/test') as ufs:
    ufs.mkdir(SafePurePosixPath())
    yield ufs
