''' A filesystem wrapper for logging.
This logs all operations it passes through to the underlying UFS.

Usage:
ufs = Logger(UFS())
'''

import logging
import typing as t
import traceback
from ufs.spec import UFS, FileStat

logger = logging.getLogger(__name__)

class Logger(UFS):
  def __init__(self, ufs: UFS):
    super().__init__()
    self._ufs = ufs
    self._logger = logger.getChild(repr(self._ufs))
    self._logger.warning(self._ufs)

  @staticmethod
  def from_dict(*, ufs):
    return Logger(
      ufs=UFS.from_dict(**ufs),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
    )

  def _call(self, op, *args, **kwargs):
    method = f"{op}({', '.join([*[repr(arg) for arg in args], *[key + '=' + repr(value) for key, value in kwargs.items()]])})"
    try:
      ret = getattr(self._ufs, op)(*args, **kwargs)
      self._logger.warning(f"{method} -> {ret}")
      return ret
    except Exception as e:
      self._logger.error(f"{method} raised {traceback.format_exc()}")
      raise e

  def ls(self, path: str) -> list[str]:
    return self._call('ls', path)

  def info(self, path: str) -> FileStat:
    return self._call('info', path)
  def open(self, path: str, mode: t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']) -> int:
    return self._call('open', path, mode)
  def seek(self, fd: int, pos: int, whence: t.Literal[0, 1, 2] = 0):
    return self._call('seek', fd, pos, whence)
  def read(self, fd: int, amnt: int) -> bytes:
    return self._call('read', fd, amnt)
  def write(self, fd: int, data: bytes) -> int:
    return self._call('write', fd, data)
  def truncate(self, fd: int, length: int):
    return self._call('truncate', fd, length)
  def close(self, fd: int):
    return self._call('close', fd)
  def unlink(self, path: str):
    return self._call('unlink', path)

  # optional
  def mkdir(self, path: str):
    return self._call('mkdir', path)
  def rmdir(self, path: str):
    return self._call('rmdir', path)
  def flush(self, fd: int):
    return self._call('flush', fd)

  # fallback
  def copy(self, src: str, dst: str):
    return self._call('copy', src, dst)

  def rename(self, src: str, dst: str):
    return self._call('rename', src, dst)

  def start(self):
    return self._call('start')

  def stop(self):
    return self._call('stop')
