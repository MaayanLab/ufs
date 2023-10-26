def ufs():
  from ufs.impl.sync import Sync
  from ufs.impl.asyn import Async
  from ufs.impl.memory import Memory
  with Sync(Async(Memory())) as ufs:
    yield ufs
