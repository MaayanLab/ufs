def ufs():
  from ufs.impl.tempdir import TemporaryDirectory
  with TemporaryDirectory() as ufs:
    yield ufs
