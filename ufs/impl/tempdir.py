''' A temporary directory of the form /tmp-??? in a ufs,
defaults to Prefix(Local(), tempfile.gettempdir()).

Usage:
with TemporaryDirectory() as ufs:
  pass
'''

import uuid
import tempfile
from ufs.spec import UFS
from ufs.impl.prefix import Prefix
from ufs.impl.local import Local
from ufs.utils.pathlib import SafePurePosixPath

class TemporaryDirectory(UFS):
  def __init__(self, ufs: UFS = Prefix(Local(), tempfile.gettempdir())):
    super().__init__()
    self._ufs = ufs

  @staticmethod
  def from_dict(*, ufs):
    return TemporaryDirectory(
      ufs=UFS.from_dict(**ufs),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
    )

  def ls(self, path):
    return self._ufs.ls(self._tmpdir / path)
  def info(self, path):
    return self._ufs.info(self._tmpdir / path)
  def open(self, path, mode, *, size_hint = None):
    return self._ufs.open(self._tmpdir / path, mode, size_hint=size_hint)
  def seek(self, fd, pos, whence = 0):
    return self._ufs.seek(fd, pos, whence)
  def read(self, fd, amnt):
    return self._ufs.read(fd, amnt)
  def write(self, fd, data):
    return self._ufs.write(fd, data)
  def truncate(self, fd, length):
    return self._ufs.truncate(fd, length)
  def close(self, fd):
    return self._ufs.close(fd)
  def unlink(self, path):
    return self._ufs.unlink(self._tmpdir / path)

  # optional
  def mkdir(self, path):
    return self._ufs.mkdir(self._tmpdir / path)
  def rmdir(self, path):
    return self._ufs.rmdir(self._tmpdir / path)
  def flush(self, fd):
    return self._ufs.flush(fd)

  # fallback
  def copy(self, src, dst):
    return self._ufs.copy(self._tmpdir / src, self._tmpdir / dst)

  def rename(self, src, dst):
    return self._ufs.rename(self._tmpdir / src, self._tmpdir / dst)

  def start(self):
    if not hasattr(self, '_tmpdir'):
      self._tmpdir = SafePurePosixPath(f"tmp-{uuid.uuid4()}")
      self._ufs.start()
      self._ufs.mkdir(self._tmpdir)

  def stop(self):
    if hasattr(self, '_tmpdir'):
      from ufs.access import shutil
      shutil.rmtree(self._ufs, self._tmpdir)
      self._ufs.stop()
      del self._tmpdir
