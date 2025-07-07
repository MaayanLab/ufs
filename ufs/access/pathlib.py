''' Implement a pathlib.Path-like interface to UFS
'''
from ufs.spec import UFS, AsyncUFS
from ufs.utils.pathlib import SafePurePosixPath, PathLike
from ufs.utils.io import RawBinaryIO, BufferedBinaryIO, BufferedIO, AsyncRawBinaryIO, AsyncBufferedBinaryIO, AsyncBufferedIO

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
  def stem(self):
    return self._path.stem

  @property
  def parent(self):
    return UPath(self._ufs, self._path.parent)

  def __str__(self):
    return str(self._path)

  def __repr__(self) -> str:
    return f"UPath({repr(self._ufs)}, {repr(self._path)})"

  def __hash__(self):
    return hash((self._ufs, self._path))

  def __eq__(self, other):
    return self._ufs is other._ufs and self._path == other._path

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

  def open(self, mode: str, encoding='utf-8', newline=b'\n'):
    if 'b' in mode:
      return BufferedBinaryIO(
        UPathBinaryIO(self._ufs, self._ufs.open(self._path, mode)),
        chunk_size=self._ufs.CHUNK_SIZE,
        newline=newline,
      )
    else:
      if mode.endswith('+'): mode_ = mode[:-1] + 'b+'
      else: mode_ = mode + 'b'
      return BufferedIO(
        UPathBinaryIO(self._ufs, self._ufs.open(self._path, mode_)),
        chunk_size=self._ufs.CHUNK_SIZE,
        encoding=encoding,
        newline=newline,
      )

  def unlink(self):
    self._ufs.unlink(self._path)

  def mkdir(self, parents=False, exist_ok=False):
    try:
      if parents:
        if self != self.parent and not self.parent.exists():
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

  def read_bytes(self) -> bytes:
    fd = self._ufs.open(self._path, 'rb')
    data = self._ufs.read(fd, -1)
    self._ufs.close(fd)
    return data

  def write_bytes(self, text: bytes) -> int:
    fd = self._ufs.open(self._path, 'wb', size_hint=len(text))
    ret = self._ufs.write(fd, text)
    self._ufs.close(fd)
    return ret

  def read_text(self, encoding='utf-8'):
    return self.read_bytes().decode(encoding)

  def write_text(self, text: str, encoding='utf-8') -> int:
    return self.write_bytes(text.encode(encoding))

  def rglob(self, pattern, *, case_sensitive=False):
    import fnmatch; _fnmatch = fnmatch.fnmatchcase if case_sensitive else fnmatch.fnmatch
    if self.is_dir():
      Q = [p for p in self.iterdir()]
      while Q:
        path = Q.pop()
        if path.is_file():
          if _fnmatch(path.name, pattern):
            yield path
        elif path.is_dir():
          if _fnmatch(path.name, pattern):
            yield path
          Q += [p for p in path.iterdir()]

class UPathBinaryIO(RawBinaryIO):
  def __init__(self, ufs: UFS, fd: int):
    self._ufs = ufs
    self._fd = fd
  def seek(self, amnt: int, whence: int = 0):
    return self._ufs.seek(self._fd, amnt, whence)
  def read(self, amnt: int = -1) -> bytes:
    return self._ufs.read(self._fd, amnt)
  def write(self, data: bytes) -> int:
    return self._ufs.write(self._fd, data)
  def flush(self):
    self._ufs.flush(self._fd)
  def truncate(self, length: int):
    self._ufs.truncate(self._fd, length)
  def close(self):
    self._ufs.close(self._fd)

class AsyncUPath:
  ''' A class implementing `pathlib.Path` methods for a `ufs`
  '''
  def __init__(self, ufs: UFS, path: PathLike = '/') -> None:
    self._ufs = ufs
    self._path = SafePurePosixPath(path)

  @property
  def name(self):
    return self._path.name

  @property
  def stem(self):
    return self._path.stem

  @property
  def parent(self):
    return AsyncUPath(self._ufs, self._path.parent)

  def __str__(self):
    return str(self._path)

  def __repr__(self) -> str:
    return f"AsyncUPath({repr(self._ufs)}, {repr(self._path)})"

  def __hash__(self):
    return hash((self._ufs, self._path))

  def __eq__(self, other):
    return self._ufs is other._ufs and self._path == other._path

  def __truediv__(self, subpath: PathLike):
    return self.__class__(self._ufs, self._path / subpath)

  async def exists(self):
    try:
      await self._ufs.info(self._path)
      return True
    except FileNotFoundError:
      return False

  async def is_file(self):
    try:
      return (await self._ufs.info(self._path))['type'] == 'file'
    except FileNotFoundError:
      return False

  async def is_dir(self):
    try:
      return (await self._ufs.info(self._path))['type'] == 'directory'
    except FileNotFoundError:
      return False

  async def open(self, mode: str, encoding='utf-8', newline=b'\n'):
    if 'b' in mode:
      return AsyncBufferedBinaryIO(
        AsyncUPathBinaryIO(self._ufs, await self._ufs.open(self._path, mode)),
        chunk_size=self._ufs.CHUNK_SIZE,
        newline=newline,
      )
    else:
      if mode.endswith('+'): mode_ = mode[:-1] + 'b+'
      else: mode_ = mode + 'b'
      return AsyncBufferedIO(
        AsyncUPathBinaryIO(self._ufs, await self._ufs.open(self._path, mode_)),
        chunk_size=self._ufs.CHUNK_SIZE,
        encoding=encoding,
        newline=newline,
      )

  async def unlink(self):
    await self._ufs.unlink(self._path)

  async def mkdir(self, parents=False, exist_ok=False):
    try:
      if parents:
        if self != self.parent and not await self.parent.exists():
          await self.parent.mkdir(parents=True)
      await self._ufs.mkdir(self._path)
    except FileExistsError as e:
      if not exist_ok: raise e

  async def rmdir(self):
    await self._ufs.rmdir(self._path)

  async def rename(self, other: str):
    if str(other).startswith('/'):
      await self._ufs.rename(self._path, SafePurePosixPath(other))
    else:
      await self._ufs.rename(self._path, self._path.parent/other)

  async def iterdir(self):
    for name in await self._ufs.ls(self._path):
      yield self / name

  async def read_bytes(self) -> bytes:
    fd = await self._ufs.open(self._path, 'rb')
    data = await self._ufs.read(fd, -1)
    await self._ufs.close(fd)
    return data

  async def write_bytes(self, text: bytes) -> int:
    fd = await self._ufs.open(self._path, 'wb', size_hint=len(text))
    ret = await self._ufs.write(fd, text)
    await self._ufs.close(fd)
    return ret

  async def read_text(self, encoding='utf-8'):
    return (await self.read_bytes()).decode(encoding)

  async def write_text(self, text: str, encoding='utf-8') -> int:
    return await self.write_bytes(text.encode(encoding))

  async def rglob(self, pattern, *, case_sensitive=False):
    import fnmatch; _fnmatch = fnmatch.fnmatchcase if case_sensitive else fnmatch.fnmatch
    if await self.is_dir():
      Q = [p async for p in self.iterdir()]
      while Q:
        path = Q.pop()
        if await path.is_file():
          if _fnmatch(path.name, pattern):
            yield path
        elif await path.is_dir():
          if _fnmatch(path.name, pattern):
            yield path
          Q += [p async for p in path.iterdir()]


class AsyncUPathBinaryIO(AsyncRawBinaryIO):
  def __init__(self, ufs: AsyncUFS, fd: int):
    self._ufs = ufs
    self._fd = fd
  async def seek(self, amnt: int, whence: int = 0):
    return await self._ufs.seek(self._fd, amnt, whence)
  async def read(self, amnt: int = -1) -> bytes:
    return await self._ufs.read(self._fd, amnt)
  async def write(self, data: bytes) -> int:
    return await self._ufs.write(self._fd, data)
  async def flush(self):
    await self._ufs.flush(self._fd)
  async def truncate(self, length: int):
    await self._ufs.truncate(self._fd, length)
  async def close(self):
    await self._ufs.close(self._fd)
