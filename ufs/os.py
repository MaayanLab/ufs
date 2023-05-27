''' Implement an os-like interface to UFS
'''
import os
import stat
import time
import typing as t
import logging
from ufs.spec import UFS

logger = logging.getLogger(__name__)

FileDescriptorLike: t.TypeAlias = int
StrPath: t.TypeAlias = str | os.PathLike[str]
StrOrBytesPath: t.TypeAlias = str | bytes | os.PathLike[str] | os.PathLike[bytes]
FileDescriptorOrPath: t.TypeAlias = int | StrOrBytesPath
ReadableBuffer: t.TypeAlias = bytes

class UOS:
  ''' A class implementing `os.` methods for a `ufs`
  '''
  def __init__(self, ufs: UFS):
    self._ufs = ufs

  def __repr__(self):
    return f"UOS({repr(self._ufs)})"

  def access(
    self,
    path: FileDescriptorOrPath,
    mode: int,
    *,
    dir_fd: int | None = None,
    effective_ids: bool = False,
    follow_symlinks: bool = True
  ) -> bool:
    try: self._ufs.info(path)
    except FileNotFoundError: return False
    except IsADirectoryError: return True
    except PermissionError: return False
    return True

  def chmod(
    self,
    path: FileDescriptorOrPath,
    mode: int,
    *,
    dir_fd: int | None = None,
    follow_symlinks: bool = True
  ) -> None:
    pass

  def chown(
    self,
    path: FileDescriptorOrPath,
    uid: int,
    gid: int,
    *,
    dir_fd: int | None = None,
    follow_symlinks: bool = True
  ) -> None:
    pass

  def open(
    self,
    path: StrOrBytesPath,
    flags: int,
    perms: int = 511,
    *,
    dir_fd: int | None = None
  ) -> int:
    if flags & os.O_TRUNC: mode = 'wb'
    elif flags & os.O_APPEND: mode = 'ab' + ('+' if flags & os.O_RDWR else '')
    elif flags & os.O_RDWR: mode = 'rb+'
    elif flags & os.O_WRONLY: mode = 'wb'
    # elif flags & os.O_RDONLY: mode = 'rb'
    else: mode = 'rb'
    logger.debug(f"open({path=}, {mode=}) {flags=}")
    return self._ufs.open(path, mode)

  def fsync(
    self,
    fd: FileDescriptorLike
  ) -> None:
    self._ufs.flush(fd)

  def fdatasync(
    self,
    fd: FileDescriptorLike
  ) -> None:
    self._ufs.flush(fd)

  def lstat(
    self,
    path: StrOrBytesPath,
    *,
    dir_fd: int | None = None
  ) -> os.stat_result:
    info = self._ufs.info(path)
    return os.stat_result([
      (stat.S_IFREG if info['type'] == 'file' else stat.S_IFDIR) | 0o755,#st_mode
      0,#st_ino
      0,#st_dev
      1 if info['type'] == 'file' else 2,#st_nlink
      int(os.environ.get('UID', 1000)),#st_uid
      int(os.environ.get('GID', 1000)),#st_gid
      info['size'] if info['type'] == 'file' else 0,#st_size
      info.get('atime', time.time()),#st_atime
      info.get('ctime', time.time()),#st_mtime
      info.get('mtime', time.time()),#st_ctime
    ])

  def link(
    self,
    src: StrOrBytesPath,
    dst: StrOrBytesPath,
    *,
    src_dir_fd: int | None = None,
    dst_dir_fd: int | None = None,
    follow_symlinks: bool = True
  ) -> None:
    pass

  def mkdir(
    self,
    path: StrOrBytesPath,
    mode: int = 511,
    *,
    dir_fd: int | None = None
  ) -> None:
    self._ufs.mkdir(path)

  def mknod(
    self,
    path: StrOrBytesPath,
    mode: int = 384,
    device: int = 0,
    *,
    dir_fd: int | None = None
  ) -> None:
    pass

  def readlink(
    self,
    path: str,
    *,
    dir_fd: int | None = None
  ) -> str:
    pass
  
  def rmdir(
    self,
    path: StrOrBytesPath,
    *,
    dir_fd: int | None = None
  ) -> None:
    self._ufs.rmdir(path)
  
  def unlink(
    self,
    path: StrOrBytesPath,
    *,
    dir_fd: int | None = None
  ) -> None:
    self._ufs.unlink(path)

  def utime(
    self,
    path: FileDescriptorOrPath,
    times: tuple[int, int] | tuple[float, float] | None = None,
    *,
    ns: tuple[int, int] = ...,
    dir_fd: int | None = None,
    follow_symlinks: bool = True
  ) -> None:
    pass

  def lseek(
    self,
    __fd: int,
    __position: int,
    __how: int,
    /
  ) -> int:
    return self._ufs.seek(__fd, __position, __how)

  def read(
    self,
    __fd: int,
    __length: int,
    /
  ) -> bytes:
    return self._ufs.read(__fd, __length)
  
  def listdir(
    self,
    path: StrPath | None = None
  ) -> list[str]:
    return self._ufs.ls(path)
  
  def close(
    self,
    fd: int
  ) -> None:
    return self._ufs.close(fd)

  def rename(
    self,
    src: StrOrBytesPath,
    dst: StrOrBytesPath,
    *,
    src_dir_fd: int | None = None,
    dst_dir_fd: int | None = None
  ) -> None:
    self._ufs.rename(src, dst)
  
  def statvfs(
    self,
    path: FileDescriptorOrPath
  ) -> os.statvfs_result:
    raise NotImplementedError()
  
  def symlink(
    self,
    src: StrOrBytesPath,
    dst: StrOrBytesPath,
    target_is_directory: bool = False,
    *,
    dir_fd: int | None = None
  ) -> None:
    pass

  def write(
    self,
    __fd: int,
    __data: ReadableBuffer,
      /
  ) -> int:
    return self._ufs.write(__fd, __data)

  def truncate(
    self,
    path: FileDescriptorOrPath,
    length: int
  ) -> None:
    fd = self._ufs.open(path, 'r+')
    self._ufs.truncate(fd, length)
    self._ufs.close(fd)
