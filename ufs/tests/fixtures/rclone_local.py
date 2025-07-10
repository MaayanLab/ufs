import pytest

def ufs():
  try:
    import requests
  except ImportError:
    pytest.skip('Install requests for rclone support')
  try:
    import shutil
    rclone = shutil.which('rclone')
    docker = shutil.which('docker')
    assert rclone or docker
  except AssertionError:
    pytest.skip('Failed find binary for running rclone')
  #
  import pathlib
  import tempfile
  from ufs.impl.rclone import RClone, serve_rclone_rcd
  from ufs.impl.prefix import Prefix
  from ufs.impl.rwcache import RWCache
  from ufs.impl.memory import Memory
  with tempfile.TemporaryDirectory() as tmp:
    tmp = pathlib.Path(tmp)
    (tmp/'data').mkdir()
    (tmp/'config').mkdir()
    (tmp/'config'/'rclone.conf').touch()
    with serve_rclone_rcd(dict(
      RCLONE_CONFIG_DIR=str(tmp/'config'),
      RCLONE_CONFIG_LOCAL_TYPE='alias',
      RCLONE_CONFIG_LOCAL_REMOTE=str(tmp/'data'),
    )) as rclone_config:
      with RWCache(Prefix(RClone(**rclone_config), '/local'), Memory()) as ufs:
        yield ufs
