''' A filesystem wrapper for prefixes in another.
This ensures all operations will be within the path prefix & exceptions masqueraded.

Usage:
ufs = Prefix(UFS(), '/some/subpath/')
'''

import os
import errno
import logging
import typing as t
from ufs.spec import UFS, FileStat
from ufs.pathlib import SafePosixPath

logger = logging.getLogger(__name__)

class Prefix(UFS):
  def __init__(self, ufs: UFS, prefix: str = '/'):
    self._ufs = ufs
    self._prefix = prefix
  def ls(self, path: str) -> list[str]:
    try:
      return self._ufs.ls(self._prefix + str(SafePosixPath(path))[1:])
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
  def info(self, path: str) -> FileStat:
    try:
      return self._ufs.info(self._prefix + str(SafePosixPath(path))[1:])
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
  def open(self, path: str, mode: t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']) -> int:
    try:
      return self._ufs.open(self._prefix + str(SafePosixPath(path))[1:], mode)
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
  def seek(self, fd: int, pos: int, whence: t.Literal[0, 1, 2] = 0):
    return self._ufs.seek(fd, pos, whence)
  def read(self, fd: int, amnt: int) -> bytes:
    return self._ufs.read(fd, amnt)
  def write(self, fd: int, data: bytes) -> int:
    return self._ufs.write(fd, data)
  def truncate(self, fd: int, length: int):
    return self._ufs.truncate(fd, length)
  def close(self, fd: int):
    return self._ufs.close(fd)
  def unlink(self, path: str):
    try:
      return self._ufs.unlink(self._prefix + str(SafePosixPath(path))[1:])
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

  # optional
  def mkdir(self, path: str):
    try:
      return self._ufs.mkdir(self._prefix + str(SafePosixPath(path))[1:])
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
  def rmdir(self, path: str):
    try:
      return self._ufs.rmdir(self._prefix + str(SafePosixPath(path))[1:])
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
  def flush(self, fd: int):
    return self._ufs.flush(fd)

  # fallback
  def copy(self, src: str, dst: str):
    try:
      return self._ufs.copy(self._prefix + str(SafePosixPath(src))[1:], self._prefix + str(SafePosixPath(dst))[1:])
    except FileNotFoundError as e:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), e.filename.replace(str(self._prefix), ''))

  def rename(self, src: str, dst: str):
    try:
      return self._ufs.rename(self._prefix + str(SafePosixPath(src))[1:], self._prefix + str(SafePosixPath(dst))[1:])
    except FileNotFoundError as e:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), e.filename.replace(str(self._prefix), ''))

  def __repr__(self) -> str:
    return f"Prefix({repr(self._ufs)}, {self._prefix})"
