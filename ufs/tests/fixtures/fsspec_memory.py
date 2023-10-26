def ufs():
  from ufs.impl.fsspec import FSSpec
  from fsspec.implementations.memory import MemoryFileSystem
  with FSSpec(MemoryFileSystem()) as ufs:
    yield ufs
