''' Like OverlayFS but with UFS -- all changes go in `upper` with fallbacks to `lower`
'''
import logging
import itertools
from ufs.spec import SyncUFS

logger = logging.getLogger(__name__)

class Overlay(SyncUFS):
  def __init__(self, lower: SyncUFS, upper: SyncUFS):
    '''
    lower: fallback read from here
    upper: writes go here
    '''
    super().__init__()
    self._lower = lower
    self._upper = upper
    self._fds = {}
    self._cfd = iter(itertools.count())

  def scope(self):
    return min([self._lower.scope(), self._upper.scope()], key=lambda s: s.value)

  @staticmethod
  def from_dict(*, lower, upper):
    return Overlay(
      lower=SyncUFS.from_dict(**lower),
      upper=SyncUFS.from_dict(**upper),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      lower=self._lower.to_dict(),
      upper=self._upper.to_dict(),
    )

  def ls(self, path):
    try:
      upper_files = self._upper.ls(path)
    except FileNotFoundError:
      upper_files = None
    try:
      lower_files = self._lower.ls(path)
    except FileNotFoundError:
      lower_files = None
    if upper_files is None and lower_files is None:
      raise FileNotFoundError(path)
    return list(set(upper_files or {}) | set(lower_files or {}))

  def info(self, path):
    try:
      return self._upper.info(path)
    except FileNotFoundError:
      return self._lower.info(path)

  def open(self, path, mode, *, size_hint = None):
    if 'r' in mode and '+' not in mode:
      try:
        provider, fd = self._upper, self._upper.open(path, mode, size_hint=size_hint)
      except FileNotFoundError:
        provider, fd = self._lower, self._lower.open(path, mode, size_hint=size_hint)
    elif '+' in mode:
      try:
        provider, fd = self._upper, self._upper.open(path, mode, size_hint=size_hint)
      except FileNotFoundError:
        from ufs.access.shutil import copyfile
        copyfile(self._lower, path, self._upper, path)
        provider, fd = self._upper, self._upper.open(path, mode, size_hint=size_hint)
    else:
      provider, fd = self._upper, self._upper.open(path, mode, size_hint=size_hint)
    cfd = next(self._cfd)
    self._fds[cfd] = (provider, fd)
    return cfd

  def seek(self, fd, pos, whence = 0):
    provider, fd = self._fds[fd]
    return provider.seek(fd, pos, whence)

  def read(self, fd, amnt):
    provider, fd = self._fds[fd]
    return provider.read(fd, amnt)

  def write(self, fd, data):
    provider, fd = self._fds[fd]
    assert provider is self._upper
    return provider.write(fd, data)

  def truncate(self, fd, length):
    provider, fd = self._fds[fd]
    return provider.truncate(fd, length)

  def flush(self, fd):
    provider, fd = self._fds[fd]
    return provider.flush(fd)

  def close(self, fd):
    provider, fd = self._fds.pop(fd)
    return provider.close(fd)

  def unlink(self, path):
    return self._upper.unlink(path)

  def mkdir(self, path):
    return self._upper.mkdir(path)

  def rmdir(self, path):
    return self._upper.rmdir(path)

  def copy(self, src, dst):
    try:
      if self._upper.info(src)['type'] == 'file':
        self._upper.copy(src, dst)
      else:
        raise IsADirectoryError(src)
    except FileNotFoundError:
      from ufs.access.shutil import copyfile
      copyfile(self._lower, src, self._upper, dst)

  def rename(self, src, dst):
    try:
      return self._upper.rename(src, dst)
    except FileNotFoundError:
      if self._lower.info(src)['type'] == 'file':
        raise PermissionError()

  def start(self):
    self._lower.start()
    self._upper.start()

  def stop(self):
    self._upper.stop()
    self._lower.stop()
