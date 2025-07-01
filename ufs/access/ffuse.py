''' Can be used as a replacement for fuse in development environments, it's quite simple:
- start: copy files to the mount directory
- stop: replicate any changes to the mount directory to the ufs and cleanup
'''
import pathlib
import contextlib
import typing as t
from collections import OrderedDict
from ufs.spec import UFS
from ufs.impl.local import Local
from ufs.impl.prefix import Prefix
from ufs.utils.pathlib import SafePurePosixPath
from ufs.access.shutil import walk, copytree, rmtree

@contextlib.contextmanager
def ffuse_mount(ufs: UFS, mount_dir: t.Optional[str | pathlib.Path] = None, readonly: bool = False):
  from ufs.utils.tempfile import TemporaryMountDirectory
  with TemporaryMountDirectory(mount_dir) as mount_dir_resolved:
    assert not any(True for _ in mount_dir_resolved.iterdir()), "mount_dir should be empty"
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
      rmtree(mount_dir_ufs, root, omit_root=True)

if __name__ == '__main__':
  import os, sys, json, pathlib, threading
  ufs = UFS.from_dict(**json.loads(os.environ.pop('UFS_SPEC')))
  mount_dir = pathlib.Path(sys.argv[1])
  assert mount_dir.exists()
  with ffuse_mount(ufs, mount_dir, bool(os.environ.pop('UFS_READONLY', ''))) as _:
    threading.Event().wait()
