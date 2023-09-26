''' Can be used as a replacement for fuse in development environments, it's quite simple:
- start: copy files to the mount directory
- stop: replicate any changes to the mount directory to the ufs and cleanup
'''
import pathlib
import tempfile
import contextlib
from collections import OrderedDict
from ufs.spec import UFS
from ufs.impl.local import Local
from ufs.impl.prefix import Prefix
from ufs.utils.pathlib import SafePurePosixPath
from ufs.access.shutil import walk, copytree, rmtree

@contextlib.contextmanager
def ffuse_mount(ufs: UFS, mount_dir: str = None, readonly: bool = False):
  mount_dir_resolved = pathlib.Path(tempfile.mkdtemp() if mount_dir is None else mount_dir)
  mount_dir_ufs = Prefix(Local(), mount_dir_resolved)
  root = SafePurePosixPath()
  copytree(ufs, root, mount_dir_ufs, root, exists_ok=True)
  if not readonly:
    before = OrderedDict(walk(mount_dir_ufs, root, dirfirst=False))
  try:
    yield mount_dir_resolved
  finally:
    if not readonly:
      after = dict(walk(mount_dir_ufs, root, dirfirst=False))
      for path in (before.keys() - after.keys()):
        item = before[path]
        if item['type'] == 'file':
          ufs.unlink(path)
        elif item['type'] == 'directory':
          ufs.rmdir(path)
      copytree(mount_dir_ufs, root, ufs, root, exists_ok=True)
    rmtree(mount_dir_ufs, root)
    if mount_dir is None:
      mount_dir_resolved.rmdir()

if __name__ == '__main__':
  import os, sys, json, pathlib, threading
  ufs = UFS.from_dict(**json.loads(os.environ.pop('UFS_SPEC')))
  mount_dir = pathlib.Path(sys.argv[1])
  assert mount_dir.exists()
  with ffuse_mount(ufs, mount_dir, bool(os.environ.pop('UFS_READONLY', ''))):
    threading.Event().wait()
