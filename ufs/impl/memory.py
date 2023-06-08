''' An in-memory filesystem
'''
import io
import time
import itertools
import dataclasses
import typing as t
from ufs.spec import UFS, FileStat
from ufs.utils.pathlib import pathparent, pathname

@dataclasses.dataclass
class MemoryInode:
  info: FileStat
  content: bytes = bytes()

@dataclasses.dataclass
class MemoryFileDescriptor:
  path: str
  stream: io.BytesIO

class Memory(UFS):
  def __init__(self) -> None:
    super().__init__()
    self._inodes: dict[str, MemoryInode] = {
      '/': MemoryInode({
        'type': 'directory',
        'size': 0,
      })
    }
    self._dirs: dict[str, set[str]] = {
      '/': set(),
    }
    self._cfd = iter(itertools.count(start=5))
    self._fds: dict[int, MemoryFileDescriptor] = {}

  def ls(self, path: str):
    try: return list(self._dirs[path])
    except KeyError: raise FileNotFoundError(path)

  def info(self, path: str) -> FileStat:
    try: return self._inodes[path].info
    except KeyError: raise FileNotFoundError(path)

  def open(self, path: str, mode: str) -> int:
    if mode.startswith('r') and path not in self._inodes: raise FileNotFoundError(path)
    if path in self._inodes and self._inodes[path].info['type'] == 'directory': raise IsADirectoryError(path)
    if pathparent(path) not in self._dirs: raise FileNotFoundError(pathparent(path))
    if path not in self._inodes:
      self._inodes[path] = MemoryInode({ 'type': 'file', 'size': 0, 'atime': time.time(), 'ctime': time.time(), 'mtime': time.time(), })
      self._dirs[pathparent(path)].add(pathname(path))
    fd = next(self._cfd)
    self._fds[fd] = MemoryFileDescriptor(path, io.BytesIO(self._inodes[path].content))
    if mode.startswith('a'): self._fds[fd].stream.seek(0, 2)
    return fd

  def seek(self, fd: int, pos: int, whence: t.Literal[0, 1, 2] = 0):
    return self._fds[fd].stream.seek(pos, whence)
  def read(self, fd: int, amnt: int = -1) -> bytes:
    return self._fds[fd].stream.read(amnt)
  def write(self, fd: int, data: bytes) -> int:
    ret = self._fds[fd].stream.write(data)
    self._inodes[self._fds[fd].path].info['size'] = len(self._fds[fd].stream.getvalue())
    return ret
  def truncate(self, fd: int, length: int):
    ret = self._fds[fd].stream.truncate(length)
    self._inodes[self._fds[fd].path].info['size'] = len(self._fds[fd].stream.getvalue())
    return ret

  def close(self, fd: int):
    descriptor = self._fds.pop(fd)
    self._inodes[descriptor.path].content = descriptor.stream.getvalue()
    self._inodes[descriptor.path].info['size'] = len(self._inodes[descriptor.path].content)

  def unlink(self, path: str):
    if path not in self._inodes: raise FileNotFoundError(path)
    elif self._inodes[path].info['type'] == 'directory': raise IsADirectoryError(path)
    else:
      self._dirs[pathparent(path)].remove(pathname(path))
      del self._inodes[path]

  def mkdir(self, path: str):
    if path in self._inodes: raise FileExistsError(path)
    if pathparent(path) not in self._dirs: raise FileNotFoundError(pathparent(path))
    self._inodes[path] = MemoryInode({ 'type': 'directory', 'size': 0 })
    self._dirs[path] = set()
    self._dirs[pathparent(path)].add(pathname(path))

  def rmdir(self, path: str):
    if path not in self._inodes: raise FileNotFoundError(path)
    if path not in self._dirs: raise NotADirectoryError(path)
    if self._dirs[path]: raise RuntimeError('Directory not Empty')
    self._dirs[pathparent(path)].remove(pathname(path))
    del self._dirs[path]
    del self._inodes[path]

  def copy(self, src: str, dst: str):
    if src not in self._inodes: raise FileNotFoundError(src)
    if self._inodes[src].info['type'] == 'directory': raise IsADirectoryError(src)
    if dst in self._inodes: raise FileExistsError()
    if pathparent(dst) not in self._dirs: raise FileNotFoundError(pathparent(dst))
    self._inodes[dst] = self._inodes[src]
    self._dirs[pathparent(dst)].add(pathname(dst))
