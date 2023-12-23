def ufs():
  from ufs.impl.fsspec import FSSpec
  from ufs.impl.memory import Memory
  from ufs.impl.process import Process
  from ufs.impl.writecache import Writecache
  from fsspec.implementations.memory import MemoryFileSystem
  with Writecache(Process(FSSpec(MemoryFileSystem())), Memory()) as ufs:
    yield ufs
