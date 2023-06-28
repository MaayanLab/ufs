''' Implement a pathlib.Path-like interface to UFS
'''
import pathlib
from ufs.spec import UFS
from ufs.utils.pathlib import SafePurePosixPath, SafePurePosixPath_, PathLike

class UPath:
  ''' A class implementing `pathlib.Path` methods for a `ufs`
  '''
  def __init__(self, ufs: UFS, path: PathLike = '/') -> None:
    self._ufs = ufs
    self._path = SafePurePosixPath(path)
  
  @property
  def name(self):
    return self._path.name

  @property
  def parent(self):
    return UPath(self._ufs, self._path.parent)

  def __str__(self):
    return str(self._path)

  def __repr__(self) -> str:
    return f"UPath({repr(self._ufs)}, {repr(self._path)})"

  def __truediv__(self, subpath: PathLike):
    return self.__class__(self._ufs, self._path / subpath)

  def exists(self):
    try:
      self._ufs.info(self._path)
      return True
    except FileNotFoundError:
      return False

  def is_file(self):
    try:
      return self._ufs.info(self._path)['type'] == 'file'
    except FileNotFoundError:
      return False

  def is_dir(self):
    try:
      return self._ufs.info(self._path)['type'] == 'directory'
    except FileNotFoundError:
      return False

  def open(self, mode: str, *, size_hint = None):
    if 'b' in mode: return UPathBinaryOpener(self._ufs, self._path, mode, size_hint=size_hint)
    else: return UPathOpener(self._ufs, self._path, mode, size_hint=size_hint)

  def unlink(self):
    self._ufs.unlink(self._path)

  def mkdir(self, parents=False, exist_ok=False):
    try:
      if parents:
        if not self.parent.exists():
          self.parent.mkdir(parents=True)
      self._ufs.mkdir(self._path)
    except FileExistsError as e:
      if not exist_ok: raise e

  def rmdir(self):
    self._ufs.rmdir(self._path)

  def rename(self, other: str):
    if str(other).startswith('/'):
      self._ufs.rename(self._path, SafePurePosixPath(other))
    else:
      self._ufs.rename(self._path, self._path.parent/other)

  def iterdir(self):
    for name in self._ufs.ls(self._path):
      yield self / name
  
  def read_text(self):
    with self.open('r') as fr:
      return fr.read()

  def write_text(self, text: str):
    self.write_bytes(text.encode())

  def read_bytes(self) -> bytes:
    with self.open('rb') as fr:
      return fr.read()

  def write_bytes(self, text: bytes):
    with self.open('wb', size_hint=len(text)) as fw:
      fw.write(text)

class UPathBinaryOpener:
  def __init__(self, ufs: UFS, path: SafePurePosixPath_, mode: str, *, size_hint: int=None):
    self._ufs = ufs
    self._path = path
    self._mode = mode
    self._fd = self._ufs.open(self._path, self._mode, size_hint=size_hint)
    self.closed = False
  def __enter__(self):
    return self
  def __exit__(self, *args):
    self.close()
  def close(self):
    self.closed = True
    self._ufs.close(self._fd)
  def seek(self, amnt: int, whence: int = 0):
    assert not self.closed
    return self._ufs.seek(self._fd, amnt, whence)
  def read(self, amnt: int = -1) -> bytes:
    assert not self.closed
    return self._ufs.read(self._fd, amnt)
  def write(self, data: bytes) -> int:
    assert not self.closed
    return self._ufs.write(self._fd, data)

class UPathOpener(UPathBinaryOpener):
  def __init__(self, ufs: UFS, path: SafePurePosixPath_, mode: str, *, size_hint: int=None):
    if mode.endswith('+'): mode_ = mode[:-1] + 'b+'
    else: mode_ = mode + 'b'
    super().__init__(ufs, path, mode_, size_hint=size_hint)
  def write(self, data: str) -> int:
    return super().write(data.encode('utf-8'))
  def read(self, amnt: int = -1) -> str:
    return super().read(amnt).decode('utf-8')

def rmtree(upath: UPath | pathlib.Path):
  ''' This doesn't exist in normal pathlib but comes in handy
  '''
  Q = [(upath, True)] + [(path, False) for path in upath.iterdir()]
  while Q:
    path, empty = Q.pop()
    if path.is_file(): path.unlink()
    elif path.is_dir():
      if empty: path.rmdir()
      else: Q += [(path, True)] + [(pp, False) for pp in path.iterdir()]
