''' pyfuse Operations on UFS interface
'''
import os
import errno
import logging
import pathlib
import contextlib
from ufs.spec import UFS
from ufs.access.os import UOS
from fuse import LoggingMixIn, Operations, FuseOSError

logger = logging.getLogger(__name__)

class FUSEOps(LoggingMixIn, Operations):
  def __init__(self, ufs: UFS, readonly = False) -> None:
    super().__init__()
    self._os = UOS(ufs)
    # self._lock = Lock()
    self._readonly = readonly

  def access(self, path, amode):
    if self._readonly and amode & os.W_OK:
      raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
    if not self._os.access(path, amode):
      raise FuseOSError(errno.EACCES)

  def chmod(self, path, *args, **kwargs):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
    return self._os.chmod(path, *args, **kwargs)

  def chown(self, path, *args, **kwargs):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
    return self._os.chown(path, *args, **kwargs)

  def create(self, path, mode):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
    return self._os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

  def flush(self, path, fh):
    return self._os.fsync(fh)

  def fsync(self, path, datasync, fh):
    if datasync != 0:
      return self._os.fdatasync(fh)
    else:
      return self._os.fsync(fh)

  def getattr(self, path, fh=None):
    st = self._os.stat(path)
    return dict((key, getattr(st, key)) for key in (
      'st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime',
      'st_nlink', 'st_size', 'st_uid'))

  def link(self, target, source):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), source)
    self._os.link(target, source)

  def mkdir(self, path, *args, **kwargs):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
    return self._os.mkdir(path, *args, **kwargs)

  def mknod(self, path, *args, **kwargs):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
    return self._os.mknod(path, *args, **kwargs)

  def open(self, path, *args, **kwargs):
    return self._os.open(path, *args, **kwargs)

  def readlink(self, path, *args, **kwargs):
    return self._os.readlink(path, *args, **kwargs)

  def rmdir(self, path, *args, **kwargs):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
    return self._os.rmdir(path, *args, **kwargs)

  def unlink(self, path, *args, **kwargs):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
    return self._os.unlink(path, *args, **kwargs)

  def utimens(self, path, *args, **kwargs):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
    return self._os.utime(path, *args, **kwargs)

  def read(self, path, size, offset, fh):
    # with self._lock:
    self._os.lseek(fh, offset, 0)
    result = self._os.read(fh, size)
    return result

  def readdir(self, path, fh):
    return ['.', '..'] + self._os.listdir(path)

  def release(self, path, fh):
    return self._os.close(fh)

  def rename(self, old, new):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), new)
    return self._os.rename(old, new)

  def statfs(self, path):
    return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

  # def statfs(self, path):
  #   stv = self._os.statvfs(path)
  #   return dict((key, getattr(stv, key)) for key in (
  #       'f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail',
  #       'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'))

  def symlink(self, target, source):
    return self._os.symlink(source, target)

  def truncate(self, path, length, fh=None):
    if self._readonly: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
    self._os.truncate(path, length)

  def write(self, path, data, offset, fh):
    # with self._lock:
    self._os.lseek(fh, offset, 0)
    result = self._os.write(fh, data)
    return result

  getxattr = None
  listxattr = None

def fuse(ufs_spec: dict, mount_dir: str, readonly: bool):
  from fuse import FUSE
  with UFS.from_dict(**ufs_spec) as ufs:
    FUSE(FUSEOps(ufs, readonly=readonly), mount_dir, nothreads=True, foreground=True)

@contextlib.contextmanager
def fuse_mount(ufs: UFS, mount_dir: str = None, readonly: bool = False):
  import signal
  import tempfile
  import functools
  import multiprocessing as mp
  from ufs.utils.process import active_process
  from ufs.utils.polling import wait_for, safe_predicate
  mp_spawn = mp.get_context('spawn')
  mount_dir_resolved = pathlib.Path(tempfile.mkdtemp() if mount_dir is None else mount_dir)
  try:
    with active_process(mp_spawn.Process(target=fuse, args=(ufs.to_dict(), str(mount_dir_resolved), readonly)), terminate_signal=signal.SIGINT):
      wait_for(functools.partial(safe_predicate, mount_dir_resolved.is_mount))
      yield mount_dir_resolved
  finally:
    wait_for(functools.partial(safe_predicate, lambda: not mount_dir_resolved.is_mount()))
    if mount_dir is None:
      mount_dir_resolved.rmdir()

if __name__ == '__main__':
  import os, sys, json, pathlib, threading
  ufs = UFS.from_dict(**json.loads(os.environ.pop('UFS_SPEC')))
  mount_dir = pathlib.Path(sys.argv[1])
  assert mount_dir.exists()
  with fuse_mount(ufs, mount_dir, bool(os.environ.pop('UFS_READONLY', ''))):
    threading.Event().wait()
