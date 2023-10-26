def ufs():
  import tempfile
  from ufs.impl.local import Local
  from ufs.impl.prefix import Prefix
  with tempfile.TemporaryDirectory() as tmp:
    with Prefix(Local(), tmp) as ufs:
      yield ufs
