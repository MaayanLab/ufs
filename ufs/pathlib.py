''' Implement a pathlib.Path-like interface to UFS
'''
import pathlib
from ufs.spec import UFS

def SafePosixPath(path: str):
  ''' This ensures the path will always be /something
  It's not possible to go above the parent, // or /./ does nothing, etc..
  '''
  p = pathlib.PurePosixPath('/')
  for part in pathlib.PurePosixPath(path).parts:
    if part == '..': p = p.parent
    elif part == '.': pass
    elif part == '': pass
    else: p = p / part
  return p

class UPath:
  ''' A class implementing `pathlib.Path` methods for a `ufs`
  '''
  def __init__(self, ufs: UFS, path: str | pathlib.PurePosixPath = '/') -> None:
    self._ufs = ufs
    self._path = SafePosixPath(path)
  
  @property
  def name(self):
    return self._path.name

  def __str__(self):
    return str(self._path)

  def __truediv__(self, subpath: str):
    return self.__class__(self._ufs, self._path / str(subpath))

  def exists(self):
    try:
      self._ufs.info(str(self._path))
      return True
    except FileNotFoundError:
      return False

  def is_file(self):
    try:
      return self._ufs.info(str(self._path))['type'] == 'file'
    except FileNotFoundError:
      return False

  def is_dir(self):
    try:
      return self._ufs.info(str(self._path))['type'] == 'directory'
    except FileNotFoundError:
      return False

  def open(self, mode: str):
    if 'b' in mode: return UPathBinaryOpener(self._ufs, str(self._path), mode)
    else: return UPathOpener(self._ufs, str(self._path), mode)

  def unlink(self):
    self._ufs.unlink(str(self._path))

  def mkdir(self):
    self._ufs.mkdir(str(self._path))

  def rmdir(self):
    self._ufs.rmdir(str(self._path))

  def rename(self, other: str):
    if str(other).startswith('/'):
      self._ufs.rename(str(self._path), str(other))
    else:
      self._ufs.rename(str(self._path), str(self._path.parent/str(other)))

  def iterdir(self):
    for name in self._ufs.ls(str(self._path)):
      yield self / name
  
  def read_text(self):
    with self.open('r') as fr:
      return fr.read()

  def write_text(self, text: str):
    with self.open('w') as fw:
      fw.write(text)

  def read_bytes(self) -> bytes:
    with self.open('rb') as fr:
      return fr.read()

  def write_bytes(self, text: bytes):
    with self.open('wb') as fw:
      fw.write(text)

class UPathBinaryOpener:
  def __init__(self, ufs: UFS, path: str, mode: str):
    self._ufs = ufs
    self._path = path
    self._mode = mode
    self._fd = self._ufs.open(self._path, self._mode)
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
  def __init__(self, ufs: UFS, path: str, mode: str):
    if mode.endswith('+'): mode_ = mode[:-1] + 'b+'
    else: mode_ = mode + 'b'
    super().__init__(ufs, path, mode_)
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
