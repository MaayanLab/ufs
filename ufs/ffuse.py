import pathlib
import tempfile
import contextlib
from collections import OrderedDict
from ufs.spec import UFS
from ufs.impl.local import Local
from ufs.impl.prefix import Prefix
from ufs.utils.pathlib import SafePurePosixPath
from ufs.shutil import walk, copytree, rmtree

@contextlib.contextmanager
def ffuse_mount(ufs: UFS, mount_dir: str = None):
  mount_dir_resolved = pathlib.Path(mount_dir or tempfile.mkdtemp())
  mount_dir_ufs = Prefix(Local(), mount_dir_resolved)
  root = SafePurePosixPath('/')
  copytree(ufs, root, mount_dir_ufs, root, exists_ok=True)
  before = OrderedDict(walk(mount_dir_ufs, root, dirfirst=False))
  try:
    yield mount_dir_resolved
  finally:
    after = dict(walk(mount_dir_ufs, root, dirfirst=False))
    for path in (before.keys() - after.keys()):
      item = before[path]
      if item['type'] == 'file':
        ufs.unlink(path)
      elif item['type'] == 'directory':
        ufs.rmdir(path)
    copytree(mount_dir_ufs, root, ufs, root, exists_ok=True)
    rmtree(mount_dir_ufs, root)
