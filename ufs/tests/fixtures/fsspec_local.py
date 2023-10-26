def ufs():
  import tempfile
  from ufs.impl.prefix import Prefix
  from ufs.impl.fsspec import FSSpec
  from fsspec.implementations.local import LocalFileSystem
  with tempfile.TemporaryDirectory() as tmp:
    with Prefix(FSSpec(LocalFileSystem()), tmp) as ufs:
      yield ufs
