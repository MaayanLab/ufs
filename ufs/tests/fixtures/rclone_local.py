import pytest

def ufs():
  try:
    import requests
  except ImportError:
    pytest.skip('Install requests for rclone support')
  else:
    try:
      import shutil
      assert shutil.which('rclone')
    except AssertionError:
      pytest.skip('rclone binary not found, skipping rclone')
    else:
      import pathlib
      import tempfile
      from ufs.impl.rclone import RClone
      from ufs.impl.prefix import Prefix
      from ufs.impl.writecache import Writecache
      from ufs.impl.memory import Memory
      from ufs.impl.logger import Logger
      with tempfile.TemporaryDirectory() as tmp:
        tmp = pathlib.Path(tmp)
        (tmp/'data').mkdir()
        (tmp/'config').mkdir()
        (tmp/'config'/'rclone.conf').touch()
        with Writecache(Prefix(Logger(RClone(dict(
          RCLONE_CONFIG_DIR=str(tmp/'config'),
          RCLONE_CONFIG_LOCAL_TYPE='alias',
          RCLONE_CONFIG_LOCAL_REMOTE=str(tmp/'data'),
        ))), '/local'), Memory()) as ufs:
          yield ufs
