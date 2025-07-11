''' The UFS generic interface for universal filesystem implementation
'''
import asyncio
import typing as t
import itertools as it
from enum import Enum
from queue import Queue
from ufs.utils.pathlib import SafePurePosixPath_

FileOpenMode: t.TypeAlias = t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']
FileSeekWhence: t.TypeAlias = t.Literal[0, 1, 2]
FileType: t.TypeAlias = t.Literal['file', 'directory']

class AccessScope(Enum):
  thread = 0
  process = 1
  system = 2
  universe = 3

class FileStat(t.TypedDict):
  type: FileType
  size: int
  atime: t.Optional[float]
  ctime: t.Optional[float]
  mtime: t.Optional[float]

class ReadableIterator:
  def __init__(self, iterator: t.Iterator[bytes]) -> None:
    self.iterator = iter(iterator)
    self.buffer = b''
    self.pos = 0

  def read(self, amnt = -1):
    while amnt == -1 or amnt > len(self.buffer):
      try:
        buf = next(self.iterator)
      except StopIteration:
        break
      self.buffer += buf
    ret = self.buffer if amnt == -1 else self.buffer[:amnt]
    self.pos += len(ret)
    self.buffer = self.buffer[len(ret):]
    return ret

class QueuedIterator(Queue):
  def __init__(self, maxsize: int = 0) -> None:
    super().__init__(maxsize)
    self.pos = 0
  def write(self, data: bytes):
    self.put(data)
    self.pos += len(data)
    return len(data)
  def close(self):
    self.put(None)
  def __iter__(self):
    while True:
      item = self.get()
      if not item: break
      yield item
      self.task_done()

class DescriptorFromAtomicMixin:
  def __init__(self) -> None:
    super().__init__()
    self._cfd = iter(it.count(start=5))
    self._fds = {}

  def open(self, path: SafePurePosixPath_, mode: FileOpenMode, *, size_hint: t.Optional[int] = None) -> int:
    if '+' in mode: raise NotImplementedError()
    if 'r' in mode:
      fd = next(self._cfd)
      self._fds[fd] = dict(mode='r', iterator=ReadableIterator(self.cat(path)))
      return fd
    elif 'w' in mode:
      import threading as t
      queued_iterator = QueuedIterator()
      thread = t.Thread(target=self.put, args=(path, queued_iterator,))
      thread.start()
      fd = next(self._cfd)
      self._fds[fd] = dict(mode='w', iterator=queued_iterator, thread=thread)
      return fd
    else:
      raise NotImplementedError(mode)

  def seek(self, fd: int, pos: int, whence: FileSeekWhence = 0) -> int:
    descriptor = self._fds[fd]
    if not (
      (whence == 0 and descriptor['iterator'].pos == pos)
      or (whence == 1 and pos == 0)
    ): raise NotImplementedError()
    return pos

  def read(self, fd: int, amnt: int) -> bytes:
    descriptor = self._fds[fd]
    if descriptor['mode'] != 'r': raise NotImplementedError()
    return descriptor['iterator'].read(amnt)

  def write(self, fd: int, data: bytes) -> int:
    descriptor = self._fds[fd]
    if descriptor['mode'] != 'w': raise NotImplementedError()
    return descriptor['iterator'].write(data)

  def truncate(self, fd: int, length: int):
    descriptor = self._fds[fd]
    raise NotImplementedError()

  def flush(self, fd: int):
    descriptor = self._fds[fd]
    if descriptor['mode'] == 'w':
      descriptor['iterator'].flush()

  def close(self, fd: int):
    descriptor = self._fds.pop(fd)
    if descriptor['mode'] == 'w':
      descriptor['iterator'].close()
      descriptor['thread'].join()

class UFS:
  CHUNK_SIZE = 5*1024

  def scope(self) -> AccessScope:
    return AccessScope.thread

  @staticmethod
  def from_dict(*, cls, **kwargs):
    import importlib
    mod, _, name = cls.rpartition('.')
    cls = getattr(importlib.import_module(mod), name)
    if cls.from_dict is UFS.from_dict: return cls(**kwargs)
    else: return cls.from_dict(**kwargs)

  def to_dict(self) -> t.Dict[str, t.Any]:
    cls = self.__class__
    return dict(cls=f"{cls.__module__}.{cls.__name__}")

class SyncUFS(UFS):
  ''' A generic class interface for universal file system implementations
  '''
  # essential
  def ls(self, path: SafePurePosixPath_) -> t.List[str]:
    raise NotImplementedError()
  def info(self, path: SafePurePosixPath_) -> FileStat:
    raise NotImplementedError()

  def open(self, path: SafePurePosixPath_, mode: FileOpenMode, *, size_hint: t.Optional[int] = None) -> int:
    raise NotImplementedError()
  def seek(self, fd: int, pos: int, whence: FileSeekWhence = 0) -> int:
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
  def cat(self, path: SafePurePosixPath_) -> t.Iterator[bytes]:
    fd = self.open(path, 'rb')
    while True:
      buf = self.read(fd, self.CHUNK_SIZE)
      if not buf: break
      yield buf
    self.close(fd)

  def put(self, path: SafePurePosixPath_, data: t.Iterator[bytes], *, size_hint: t.Optional[int] = None):
    fd = self.open(path, 'wb', size_hint=size_hint)
    for buf in data:
      self.write(fd, buf)
    self.close(fd)

  def copy(self, src: SafePurePosixPath_, dst: SafePurePosixPath_):
    src_info = self.info(src)
    if src_info['type'] != 'file':
      raise IsADirectoryError(str(src))
    self.put(dst, self.cat(src), size_hint=src_info['size'])

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

class ReadableAsyncIterator:
  def __init__(self, iterator: t.AsyncIterator[bytes]) -> None:
    self.iterator = aiter(iterator)
    self.buffer = b''
    self.pos = 0
  async def read(self, amnt = -1):
    while amnt == -1 or amnt > len(self.buffer):
      try:
        buf = await anext(self.iterator)
      except StopAsyncIteration:
        break
      self.buffer += buf
    ret = self.buffer if amnt == -1 else self.buffer[:amnt]
    self.pos += len(ret)
    self.buffer = self.buffer[len(ret):]
    return ret

class QueuedAsyncIterator(asyncio.Queue):
  def __init__(self, maxsize: int = 0) -> None:
    super().__init__(maxsize)
    self.pos = 0
  async def write(self, data: bytes):
    await self.put(data)
    self.pos += len(data)
    return len(data)
  async def flush(self):
    pass
  async def close(self):
    await self.put(None)
  async def __aiter__(self) -> t.AsyncIterator[bytes]:
    while True:
      item = await self.get()
      if item is None: break
      yield item
      self.task_done()

class AsyncDescriptorFromAtomicMixin:
  def __init__(self) -> None:
    super().__init__()
    self._cfd = iter(it.count(start=5))
    self._fds = {}

  async def open(self, path: SafePurePosixPath_, mode: FileOpenMode, *, size_hint: t.Optional[int] = None) -> int:
    if '+' in mode: raise NotImplementedError()
    if 'r' in mode:
      fd = next(self._cfd)
      self._fds[fd] = dict(mode='r', iterator=ReadableAsyncIterator(self.cat(path)))
      return fd
    elif 'w' in mode:
      queued_iterator = QueuedAsyncIterator()
      task = asyncio.create_task(self.put(path, queued_iterator.__aiter__(), size_hint=size_hint))
      fd = next(self._cfd)
      self._fds[fd] = dict(mode='w', iterator=queued_iterator, task=task)
      return fd
    else:
      raise NotImplementedError(mode)

  async def seek(self, fd: int, pos: int, whence: FileSeekWhence = 0) -> int:
    descriptor = self._fds[fd]
    if not (
      (whence == 0 and descriptor['iterator'].pos == pos)
      or (whence == 1 and pos == 0)
    ): raise NotImplementedError()
    return pos

  async def read(self, fd: int, amnt: int) -> bytes:
    descriptor = self._fds[fd]
    if descriptor['mode'] != 'r': raise NotImplementedError()
    return await descriptor['iterator'].read(amnt)

  async def write(self, fd: int, data: bytes) -> int:
    descriptor = self._fds[fd]
    if descriptor['mode'] != 'w': raise NotImplementedError()
    return await descriptor['iterator'].write(data)

  async def truncate(self, fd: int, length: int):
    descriptor = self._fds[fd]
    raise NotImplementedError()

  async def flush(self, fd: int):
    descriptor = self._fds[fd]
    if descriptor['mode'] == 'w':
      await descriptor['iterator'].flush()

  async def close(self, fd: int):
    descriptor = self._fds.pop(fd)
    if descriptor['mode'] == 'w':
      await descriptor['iterator'].close()
      await descriptor['task']

class AsyncUFS(UFS):
  ''' A generic class interface for universal file system implementations
  '''
  # essential
  async def ls(self, path: SafePurePosixPath_) -> t.List[str]:
    raise NotImplementedError()
  async def info(self, path: SafePurePosixPath_) -> FileStat:
    raise NotImplementedError()
  async def open(self, path: SafePurePosixPath_, mode: FileOpenMode, *, size_hint: t.Optional[int] = None) -> int:
    raise NotImplementedError()
  async def seek(self, fd: int, pos: int, whence: FileSeekWhence = 0) -> int:
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

  async def cat(self, path: SafePurePosixPath_) -> t.AsyncIterator[bytes]:
    fd = await self.open(path, 'rb')
    while True:
      buf = await self.read(fd, self.CHUNK_SIZE)
      if not buf: break
      yield buf
    await self.close(fd)

  async def put(self, path: SafePurePosixPath_, data: t.AsyncIterator[bytes], *, size_hint: t.Optional[int] = None):
    fd = await self.open(path, 'wb', size_hint=size_hint)
    async for buf in data:
      await self.write(fd, buf)
    await self.close(fd)

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
    await self.put(dst, self.cat(src), size_hint=src_info['size'])

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
