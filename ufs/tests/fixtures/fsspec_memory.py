def ufs():
  # We use a temporary in fsspec memory otherwise we clash across tests
  from ufs.impl.fsspec import FSSpec
  from ufs.impl.tempdir import TemporaryDirectory
  from fsspec.implementations.memory import MemoryFileSystem
  with TemporaryDirectory(FSSpec(MemoryFileSystem())) as ufs:
    yield ufs
