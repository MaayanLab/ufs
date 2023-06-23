''' Implement an os-like interface to UFS
'''
import os
import stat
import time
import errno
import typing as t
import logging
import traceback
import contextlib
from ufs.spec import UFS
from ufs.utils.pathlib import SafePurePosixPath, pathparent

logger = logging.getLogger(__name__)

FileDescriptorLike: t.TypeAlias = int
StrPath: t.TypeAlias = str | os.PathLike[str]
StrOrBytesPath: t.TypeAlias = str | bytes | os.PathLike[str] | os.PathLike[bytes]
FileDescriptorOrPath: t.TypeAlias = int | StrOrBytesPath
ReadableBuffer: t.TypeAlias = bytes

@contextlib.contextmanager
def oserror(path: str = None):
  try:
    yield
  except FileNotFoundError: raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
  except FileExistsError: raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), path)
  except NotADirectoryError: raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), path)
  except IsADirectoryError: raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), path)
  except PermissionError: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path)
  except NotImplementedError: raise OSError(errno.ENOTSUP, os.strerror(errno.ENOTSUP), path)
  except:
    logger.error(traceback.format_exc())
    raise OSError(errno.ENOTSUP, os.strerror(errno.ENOTSUP), path)

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
    try: info = self._ufs.info(SafePurePosixPath(path))
    except: return False
    else: return True

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
    with oserror(path):
      logger.debug(f"open({path=}, {mode=}) {flags=}")
      return self._ufs.open(SafePurePosixPath(path), mode)

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

  def stat(
    self,
    path: StrOrBytesPath,
    *,
    dir_fd: int | None = None
  ) -> os.stat_result:
    with oserror(path):
      info = self._ufs.info(SafePurePosixPath(path))
      nlink = 2 + len(self._ufs.ls(SafePurePosixPath(path))) if info['type'] == 'directory' else 1
      return os.stat_result([
        (stat.S_IFREG | 0o644 if info['type'] == 'file' else stat.S_IFDIR | 0o755),#st_mode
        0,#st_ino
        0,#st_dev
        nlink,#st_nlink
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
    with oserror(path):
      self._ufs.mkdir(SafePurePosixPath(path))

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
    with oserror(path):
      self._ufs.rmdir(SafePurePosixPath(path))
  
  def unlink(
    self,
    path: StrOrBytesPath,
    *,
    dir_fd: int | None = None
  ) -> None:
    with oserror(path):
      self._ufs.unlink(SafePurePosixPath(path))

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
    with oserror(path):
      return self._ufs.ls(SafePurePosixPath(path))
  
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
    try:
      self._ufs.rename(SafePurePosixPath(src), SafePurePosixPath(dst))
    except FileNotFoundError: raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), src)
    except FileExistsError: raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), dst)
    except NotADirectoryError: raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), pathparent(dst))
    except IsADirectoryError: raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), dst)
    except PermissionError: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM))
    except NotImplementedError: raise OSError(errno.ENOTSUP, os.strerror(errno.ENOTSUP))
    except:
      logger.error(traceback.format_exc())
      raise OSError(errno.ENOTSUP, os.strerror(errno.ENOTSUP))
  
  def statvfs(
    self,
    path: FileDescriptorOrPath
  ) -> os.statvfs_result:
    with oserror(path):
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
    with oserror(path):
      fd = self._ufs.open(SafePurePosixPath(path), 'r+')
      self._ufs.truncate(fd, length)
      self._ufs.close(fd)
