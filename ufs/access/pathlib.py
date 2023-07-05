''' Implement a pathlib.Path-like interface to UFS
'''
from ufs.spec import UFS, AsyncUFS
from ufs.utils.pathlib import SafePurePosixPath, PathLike

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
    if 'b' in mode:
      return UPathBinaryOpener(self._ufs, self._ufs.open(self._path, mode, size_hint=size_hint))
    else:
      if mode.endswith('+'): mode_ = mode[:-1] + 'b+'
      else: mode_ = mode + 'b'
      return UPathOpener(self._ufs, self._ufs.open(self._path, mode_, size_hint=size_hint))

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
  def __init__(self, ufs: UFS, fd: int):
    self._ufs = ufs
    self._fd = fd
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
  def __iter__(self):
    assert not self.closed
    buffer = b''
    while True:
      buf = self._ufs.read(self._fd, self._ufs.CHUNK_SIZE)
      if not buf:
        break
      buffer += buf
      while True:
        line, sep, buffer = buffer.partition(b'\n')
        if not sep:
          buffer = line
          break
        yield line
    if buffer:
      yield buffer

class UPathOpener(UPathBinaryOpener):
  def write(self, data: str) -> int:
    return super().write(data.encode('utf-8'))
  def read(self, amnt: int = -1) -> str:
    return super().read(amnt).decode('utf-8')
  def __iter__(self):
    for line in super().__iter__():
      yield line.decode('utf-8')

class AsyncUPath:
  ''' A class implementing `pathlib.Path` methods for a `ufs`
  '''
  def __init__(self, ufs: AsyncUFS, path: PathLike = '/') -> None:
    self._ufs = ufs
    self._path = SafePurePosixPath(path)
  
  @property
  def name(self):
    return self._path.name

  @property
  def parent(self):
    return AsyncUPath(self._ufs, self._path.parent)

  def __str__(self):
    return str(self._path)

  def __repr__(self) -> str:
    return f"AsyncUPath({repr(self._ufs)}, {repr(self._path)})"

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
      return (await self._ufs.info(self._path)['type']) == 'file'
    except FileNotFoundError:
      return False

  async def is_dir(self):
    try:
      return (await self._ufs.info(self._path)['type']) == 'directory'
    except FileNotFoundError:
      return False

  async def open(self, mode: str, *, size_hint = None):
    if 'b' in mode:
      return AsyncUPathBinaryOpener(self._ufs, await self._ufs.open(self._path, mode, size_hint=size_hint))
    else:
      if mode.endswith('+'): mode_ = mode[:-1] + 'b+'
      else: mode_ = mode + 'b'
      return AsyncUPathOpener(self._ufs, await self._ufs.open(self._path, mode_, size_hint=size_hint))

  async def unlink(self):
    await self._ufs.unlink(self._path)

  async def mkdir(self, parents=False, exist_ok=False):
    try:
      if parents:
        if not await self.parent.exists():
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
  
  async def read_text(self):
    async with await self.open('r') as fr:
      return await fr.read()

  async def write_text(self, text: str):
    await self.write_bytes(text.encode())

  async def read_bytes(self) -> bytes:
    async with await self.open('rb') as fr:
      return await fr.read()

  async def write_bytes(self, text: bytes):
    async with await self.open('wb', size_hint=len(text)) as fw:
      await fw.write(text)

class AsyncUPathBinaryOpener:
  def __init__(self, ufs: AsyncUFS, fd: int):
    self._ufs = ufs
    self._fd = fd
    self.closed = False
  async def __aenter__(self):
    return self
  async def __aexit__(self, *args):
    await self.close()
  async def close(self):
    self.closed = True
    await self._ufs.close(self._fd)
  async def seek(self, amnt: int, whence: int = 0):
    assert not self.closed
    return await self._ufs.seek(self._fd, amnt, whence)
  async def read(self, amnt: int = -1) -> bytes:
    assert not self.closed
    return await self._ufs.read(self._fd, amnt)
  async def write(self, data: bytes) -> int:
    assert not self.closed
    return await self._ufs.write(self._fd, data)
  async def __aiter__(self):
    assert not self.closed
    buffer = b''
    while True:
      buf = await self._ufs.read(self._fd, self._ufs.CHUNK_SIZE)
      if not buf:
        break
      buffer += buf
      while True:
        line, sep, buffer = buffer.partition(b'\n')
        if not sep:
          buffer = line
          break
        yield line
    if buffer:
      yield buffer

class AsyncUPathOpener(UPathBinaryOpener):
  async def write(self, data: str) -> int:
    return await super().write(data.encode('utf-8'))
  async def read(self, amnt: int = -1) -> str:
    return (await super().read(amnt)).decode('utf-8')
  async def __aiter__(self):
    async for line in super().__iter__():
      yield line.decode('utf-8')
