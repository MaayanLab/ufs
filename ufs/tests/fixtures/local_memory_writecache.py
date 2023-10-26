def ufs():
  import tempfile
  from ufs.impl.local import Local
  from ufs.impl.prefix import Prefix
  from ufs.impl.memory import Memory
  from ufs.impl.writecache import Writecache
  with tempfile.TemporaryDirectory() as tmp:
    with Writecache(Prefix(Local(), tmp), Memory()) as ufs:
      yield ufs
