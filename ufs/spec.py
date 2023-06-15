''' The UFS generic interface for universal filesystem implementation
'''
import typing as t
from ufs.utils.pathlib import SafePurePosixPath_

FileOpenMode: t.TypeAlias = t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']
FileSeekWhence: t.TypeAlias = t.Literal[0, 1, 2]

class FileStat(t.TypedDict):
  type: t.Literal['file', 'directory']
  size: int
  atime: t.Optional[float]
  ctime: t.Optional[float]
  mtime: t.Optional[float]

class UFS:
  ''' A generic class interface for universal file system implementations
  '''
  CHUNK_SIZE = 5*1024

  @staticmethod
  def from_dict(*, cls, **kwargs):
    import importlib
    mod, _, name = cls.rpartition('.')
    cls = getattr(importlib.import_module(mod), name)
    if cls.from_dict is UFS.from_dict: return cls(**kwargs)
    else: return cls.from_dict(**kwargs)

  def to_dict(self) -> dict[str, t.Any]:
    cls = self.__class__
    return dict(cls=f"{cls.__module__}.{cls.__name__}")

  # essential
  def ls(self, path: SafePurePosixPath_) -> list[str]:
    raise NotImplementedError()
  def info(self, path: SafePurePosixPath_) -> FileStat:
    raise NotImplementedError()
  def open(self, path: SafePurePosixPath_, mode: FileOpenMode, *, size_hint: int = None) -> int:
    raise NotImplementedError()
  def seek(self, fd: int, pos: int, whence: FileSeekWhence = 0):
    raise NotImplementedError()
  def read(self, fd: int, amnt: int) -> bytes:
    raise NotImplementedError()
  def write(self, fd: int, data: bytes) -> int:
    raise NotImplementedError()
  def truncate(self, fd: int, length: int):
    raise NotImplementedError()
  def close(self, fd: int):
    raise NotImplementedError()
  def unlink(self, path: SafePurePosixPath_):
    raise NotImplementedError()

  # optional
  def mkdir(self, path: SafePurePosixPath_):
    pass
  def rmdir(self, path: SafePurePosixPath_):
    pass
  def flush(self, fd: int):
    pass
  def start(self):
    pass
  def stop(self):
    pass

  # fallback
  def copy(self, src: SafePurePosixPath_, dst: SafePurePosixPath_):
    src_info = self.info(src)
    if src_info['type'] != 'file':
      raise IsADirectoryError(str(src))
    src_fd = self.open(src, 'rb')
    dst_fd = self.open(dst, 'wb', size_hint=src_info['size'])
    while buf := self.read(src_fd, self.CHUNK_SIZE):
      self.write(dst_fd, buf)
    self.close(dst)
    self.close(src)

  def rename(self, src: SafePurePosixPath_, dst: SafePurePosixPath_):
    self.copy(src, dst)
    self.unlink(src)

  def __enter__(self):
    self.start()
    return self

  def __exit__(self, *args):
    self.stop()

  def __repr__(self) -> str:
    return f"UFS({repr(self.to_dict())})"

class AsyncUFS:
  ''' A generic class interface for universal file system implementations
  '''
  CHUNK_SIZE = 5*1024

  @staticmethod
  def from_dict(*, cls, **kwargs):
    import importlib
    mod, _, name = cls.rpartition('.')
    cls = getattr(importlib.import_module(mod), name)
    if cls.from_dict is UFS.from_dict: return cls(**kwargs)
    else: return cls.from_dict(**kwargs)

  def to_dict(self) -> dict[str, t.Any]:
    cls = self.__class__
    return dict(cls=f"{cls.__module__}.{cls.__name__}")

  # essential
  async def ls(self, path: SafePurePosixPath_) -> list[str]:
    raise NotImplementedError()
  async def info(self, path: SafePurePosixPath_) -> FileStat:
    raise NotImplementedError()
  async def open(self, path: SafePurePosixPath_, mode: FileOpenMode, *, size_hint: int = None) -> int:
    raise NotImplementedError()
  async def seek(self, fd: int, pos: int, whence: FileSeekWhence = 0):
    raise NotImplementedError()
  async def read(self, fd: int, amnt: int) -> bytes:
    raise NotImplementedError()
  async def write(self, fd: int, data: bytes) -> int:
    raise NotImplementedError()
  async def truncate(self, fd: int, length: int):
    raise NotImplementedError()
  async def close(self, fd: int):
    raise NotImplementedError()
  async def unlink(self, path: SafePurePosixPath_):
    raise NotImplementedError()

  # optional
  async def mkdir(self, path: SafePurePosixPath_):
    pass
  async def rmdir(self, path: SafePurePosixPath_):
    pass
  async def flush(self, fd: int):
    pass
  async def start(self):
    pass
  async def stop(self):
    pass

  # fallback
  async def copy(self, src: SafePurePosixPath_, dst: SafePurePosixPath_):
    src_info = await self.info(src)
    if src_info['type'] != 'file':
      raise IsADirectoryError(str(src))
    src_fd = await self.open(src, 'rb')
    dst_fd = await self.open(dst, 'wb', size_hint=src_info['size'])
    while buf := await self.read(src_fd, self.CHUNK_SIZE):
      await self.write(dst_fd, buf)
    await self.close(dst)
    await self.close(src)

  async def rename(self, src: SafePurePosixPath_, dst: SafePurePosixPath_):
    await self.copy(src, dst)
    await self.unlink(src)

  async def __aenter__(self):
    await self.start()
    return self

  async def __aexit__(self, *args):
    await self.stop()

  def __repr__(self) -> str:
    return f"AsyncUFS({repr(self.to_dict())})"
