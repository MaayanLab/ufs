''' Implement an os-like interface to UFS
'''
import os
import stat
import time
import errno
import typing as t
import logging
import contextlib
from ufs.spec import SyncUFS, FileSeekWhence
from ufs.utils.pathlib import SafePurePosixPath, pathparent

logger = logging.getLogger(__name__)

FileDescriptorLike = int
StrPath = t.Union[str, os.PathLike]
StrOrBytesPath = t.Union[str, bytes, os.PathLike]
FileDescriptorOrPath = t.Union[int, StrOrBytesPath]
ReadableBuffer = bytes

@contextlib.contextmanager
def oserror(path: t.Optional[FileDescriptorOrPath] = None):
  try:
    yield
  except FileNotFoundError as e: raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path) from e
  except FileExistsError as e: raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), path) from e
  except NotADirectoryError as e: raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), path) from e
  except IsADirectoryError as e: raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), path) from e
  except PermissionError as e: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM), path) from e
  except OSError as e: raise e
  except NotImplementedError as e: raise OSError(errno.ENOTSUP, os.strerror(errno.ENOTSUP), path) from e
  except Exception as e: raise OSError(errno.EROFS, os.strerror(errno.EROFS), path) from e

class UOS:
  ''' A class implementing `os.` methods for a `ufs`
  '''
  def __init__(self, ufs: SyncUFS):
    self._ufs = ufs

  def __repr__(self):
    return f"UOS({repr(self._ufs)})"

  def access(
    self,
    path: FileDescriptorOrPath,
    mode: int,
    *,
    dir_fd: t.Optional[int] = None,
    effective_ids: bool = False,
    follow_symlinks: bool = True
  ) -> bool:
    with oserror(path):
      if isinstance(path, int): raise NotImplementedError()
      elif isinstance(path, os.PathLike): raise NotImplementedError()
      info = self._ufs.info(SafePurePosixPath(path))
      return info is not None

  def chmod(
    self,
    path: FileDescriptorOrPath,
    mode: int,
    *,
    dir_fd: t.Optional[int] = None,
    follow_symlinks: bool = True
  ) -> None:
    with oserror(path):
      raise NotImplementedError()

  def chown(
    self,
    path: FileDescriptorOrPath,
    uid: int,
    gid: int,
    *,
    dir_fd: t.Optional[int] = None,
    follow_symlinks: bool = True
  ) -> None:
    with oserror(path):
      raise NotImplementedError()

  def open(
    self,
    path: StrOrBytesPath,
    flags: int,
    perms: int = 511,
    *,
    dir_fd: t.Optional[int] = None
  ) -> int:
    with oserror(path):
      if flags & os.O_TRUNC: mode = 'wb'
      elif flags & os.O_APPEND: mode = 'ab' + ('+' if flags & os.O_RDWR else '')
      elif flags & os.O_RDWR: mode = 'rb+'
      elif flags & os.O_WRONLY: mode = 'wb'
      # elif flags & os.O_RDONLY: mode = 'rb'
      else: mode = 'rb'
      logger.debug(f"open({path}, {mode}) {flags}")
      return self._ufs.open(SafePurePosixPath(path), mode)

  def fsync(
    self,
    fd: FileDescriptorLike
  ) -> None:
    with oserror(fd):
      self._ufs.flush(fd)

  def fdatasync(
    self,
    fd: FileDescriptorLike
  ) -> None:
    with oserror(fd):
      self._ufs.flush(fd)

  def stat(
    self,
    path: StrOrBytesPath,
    *,
    dir_fd: t.Optional[int] = None
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
        info.get('atime') or time.time(),#st_atime
        info.get('ctime') or time.time(),#st_mtime
        info.get('mtime') or time.time(),#st_ctime
      ])

  def link(
    self,
    src: StrOrBytesPath,
    dst: StrOrBytesPath,
    *,
    src_dir_fd: t.Optional[int] = None,
    dst_dir_fd: t.Optional[int] = None,
    follow_symlinks: bool = True
  ) -> None:
    with oserror(dst):
      raise NotImplementedError()

  def mkdir(
    self,
    path: StrOrBytesPath,
    mode: int = 511,
    *,
    dir_fd: t.Optional[int] = None
  ) -> None:
    with oserror(path):
      self._ufs.mkdir(SafePurePosixPath(path))

  def mknod(
    self,
    path: StrOrBytesPath,
    mode: int = 384,
    device: int = 0,
    *,
    dir_fd: t.Optional[int] = None
  ) -> None:
    with oserror(path):
      raise NotImplementedError()

  def readlink(
    self,
    path: str,
    *,
    dir_fd: t.Optional[int] = None
  ) -> str:
    raise OSError(errno.ENOLINK)
  
  def rmdir(
    self,
    path: StrOrBytesPath,
    *,
    dir_fd: t.Optional[int] = None
  ) -> None:
    with oserror(path):
      self._ufs.rmdir(SafePurePosixPath(path))
  
  def unlink(
    self,
    path: StrOrBytesPath,
    *,
    dir_fd: t.Optional[int] = None
  ) -> None:
    with oserror(path):
      self._ufs.unlink(SafePurePosixPath(path))

  def utime(
    self,
    path: FileDescriptorOrPath,
    times: t.Union[t.Tuple[int, int], t.Tuple[float, float], None] = None,
    *,
    ns: t.Tuple[int, int],
    dir_fd: t.Optional[int] = None,
    follow_symlinks: bool = True
  ) -> None:
    with oserror(path):
      raise NotImplementedError()

  def lseek(
    self,
    __fd: int,
    __position: int,
    __how: FileSeekWhence = 0,
  ) -> int:
    with oserror(__fd):
      return self._ufs.seek(__fd, __position, __how)

  def read(
    self,
    __fd: int,
    __length: int,
  ) -> bytes:
    with oserror(__fd):
      return self._ufs.read(__fd, __length)
  
  def listdir(
    self,
    path: t.Optional[StrPath] = None
  ) -> t.List[str]:
    with oserror(path):
      return self._ufs.ls(SafePurePosixPath(path))
  
  def close(
    self,
    fd: int
  ) -> None:
    with oserror(fd):
      return self._ufs.close(fd)

  def rename(
    self,
    src: StrOrBytesPath,
    dst: StrOrBytesPath,
    *,
    src_dir_fd: t.Optional[int] = None,
    dst_dir_fd: t.Optional[int] = None
  ) -> None:
    try:
      self._ufs.rename(SafePurePosixPath(src), SafePurePosixPath(dst))
    except FileNotFoundError as e: raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), src) from e
    except FileExistsError as e: raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), dst) from e
    except NotADirectoryError as e: raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), pathparent(str(dst))) from e
    except IsADirectoryError as e: raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), dst) from e
    except PermissionError as e: raise PermissionError(errno.EPERM, os.strerror(errno.EPERM)) from e
    except NotImplementedError as e: raise OSError(errno.ENOTSUP, os.strerror(errno.ENOTSUP)) from e
    except Exception as e: raise OSError(errno.ENOTSUP, os.strerror(errno.ENOTSUP)) from e
  
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
    dir_fd: t.Optional[int] = None
  ) -> None:
    with oserror(dst):
      raise NotImplementedError()

  def write(
    self,
    __fd: int,
    __data: ReadableBuffer,
  ) -> int:
    with oserror(__fd):
      return self._ufs.write(__fd, __data)

  def truncate(
    self,
    path: FileDescriptorOrPath,
    length: int
  ) -> None:
    with oserror(path):
      if isinstance(path, int):
        fd = path
        self._ufs.truncate(fd, length)
      else:      
        fd = self._ufs.open(SafePurePosixPath(path), 'rb+')
        self._ufs.truncate(fd, length)
        self._ufs.close(fd)
