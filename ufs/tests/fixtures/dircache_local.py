def ufs():
  import tempfile
  from ufs.impl.local import Local
  from ufs.impl.prefix import Prefix
  from ufs.impl.dircache import DirCache
  with tempfile.TemporaryDirectory() as tmp:
    with DirCache(Prefix(Local(), tmp)) as ufs:
      yield ufs
