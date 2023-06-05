''' An in-memory filesystem
'''
import io
import time
import itertools
import dataclasses
from ufs.pathlib import SafePosixPath
from ufs.spec import UFS, FileStat

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
    self._fds: dict[int, io.BytesIO] = {}

  def ls(self, path: str):
    try: return list(self._dirs[path])
    except KeyError: raise FileNotFoundError(path)

  def info(self, path: str) -> FileStat:
    try: return self._inodes[path].info
    except KeyError: raise FileNotFoundError(path)

  def open(self, path: str, mode: str) -> int:
    if mode.startswith('r') and path not in self._inodes: raise FileNotFoundError(path)
    if path in self._inodes and self._inodes[path].info['type'] == 'directory': raise IsADirectoryError(path)
    posix_path = SafePosixPath(path)
    if str(posix_path.parent) not in self._dirs: raise FileNotFoundError(str(posix_path.parent))
    if path not in self._inodes:
      self._inodes[path] = MemoryInode({ 'type': 'file', 'size': 0, 'atime': time.time(), 'ctime': time.time(), 'mtime': time.time(), })
      self._dirs[str(posix_path.parent)].add(posix_path.name)
    fd = next(self._cfd)
    self._fds[fd] = MemoryFileDescriptor(path, io.BytesIO(self._inodes[path].content))
    if mode.startswith('a'): self._fds[fd].stream.seek(0, 2)
    return fd

  def seek(self, fd: int, pos: int, whence: int):
    return self._fds[fd].stream.seek(pos, whence)
  def read(self, fd: int, amnt: int = -1) -> bytes:
    return self._fds[fd].stream.read(amnt)
  def write(self, fd: int, data: bytes) -> int:
    return self._fds[fd].stream.write(data)
  def truncate(self, fd: int, length: int):
    self._fds[fd].stream.truncate(length)

  def close(self, fd: int):
    descriptor = self._fds.pop(fd)
    self._inodes[descriptor.path].content = descriptor.stream.getvalue()
    self._inodes[descriptor.path].info['size'] = len(self._inodes[descriptor.path].content)

  def unlink(self, path: str):
    if path not in self._inodes: raise FileNotFoundError(path)
    elif self._inodes[path].info['type'] == 'directory': raise IsADirectoryError(path)
    else:
      posix_path = SafePosixPath(path)
      self._dirs[str(posix_path.parent)].remove(posix_path.name)
      del self._inodes[path]

  def mkdir(self, path: str):
    if path in self._inodes: raise FileExistsError(path)
    posix_path = SafePosixPath(path)
    if str(posix_path.parent) not in self._dirs: raise FileNotFoundError(str(posix_path.parent))
    self._inodes[path] = MemoryInode({ 'type': 'directory', 'size': 0 })
    self._dirs[path] = set()
    self._dirs[str(posix_path.parent)].add(posix_path.name)

  def rmdir(self, path: str):
    if path not in self._inodes: raise FileNotFoundError(path)
    if path not in self._dirs: raise NotADirectoryError(path)
    if self._dirs[path]: raise RuntimeError('Directory not Empty')
    posix_path = SafePosixPath(path)
    self._dirs[str(posix_path.parent)].remove(posix_path.name)
    del self._dirs[path]
    del self._inodes[path]

  def copy(self, src: str, dst: str):
    if src not in self._inodes: raise FileNotFoundError(src)
    if self._inodes[src].info['type'] == 'directory': raise IsADirectoryError(src)
    if dst in self._inodes: raise FileExistsError()
    posix_dst = SafePosixPath(dst)
    if str(posix_dst.parent) not in self._dirs: raise FileNotFoundError(str(posix_dst.parent))
    self._inodes[dst] = self._inodes[src]
    self._dirs[str(posix_dst.parent)].add(posix_dst.name)
