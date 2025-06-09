def ufs():
  # We use a temporary in fsspec memory otherwise we clash across tests
  from ufs.impl.fsspec import FSSpec
  from ufs.impl.tempdir import TemporaryDirectory
  from ufs.impl.memory import Memory
  from ufs.impl.writecache import Writecache
  from fsspec.implementations.memory import MemoryFileSystem
  with TemporaryDirectory(Writecache(FSSpec(MemoryFileSystem()), Memory())) as ufs:
    yield ufs
