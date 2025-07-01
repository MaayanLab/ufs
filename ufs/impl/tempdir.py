''' A temporary directory of the form /tmp-??? in a ufs,
defaults to Prefix(Local(), tempfile.gettempdir()).

Usage:
with TemporaryDirectory() as ufs:
  pass
'''

import uuid
import typing as t
from ufs.spec import SyncUFS
from ufs.impl.prefix import Prefix
from ufs.impl.local import Local
from ufs.utils.pathlib import SafePurePosixPath, SafePurePosixPath_

class TemporaryDirectory(SyncUFS):
  def __init__(self, ufs: t.Optional[SyncUFS] = None, tmpdir: t.Optional[SafePurePosixPath_] = None):
    super().__init__()
    self._ufs = Prefix(Local(), '/tmp') if ufs is None else ufs
    self._tmpdir = SafePurePosixPath(str(uuid.uuid4())) if tmpdir is None else tmpdir
    self._outer = tmpdir is None

  def scope(self):
    return self._ufs.scope()

  @staticmethod
  def from_dict(*, ufs, tmpdir):
    return TemporaryDirectory(
      ufs=SyncUFS.from_dict(**ufs),
      tmpdir=SafePurePosixPath(tmpdir),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
      tmpdir=str(self._tmpdir),
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
    self._ufs.start()
    try: self._ufs.mkdir(SafePurePosixPath())
    except IsADirectoryError: pass
    except FileExistsError: pass
    try: self._ufs.mkdir(SafePurePosixPath(self._tmpdir))
    except IsADirectoryError: pass
    except FileExistsError: pass

  def stop(self):
    from ufs.access.shutil import rmtree
    if self._outer: rmtree(self._ufs, self._tmpdir)
    self._ufs.stop()
