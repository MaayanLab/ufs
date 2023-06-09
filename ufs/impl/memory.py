''' An in-memory filesystem
'''
import io
import time
import itertools
import dataclasses
from ufs.spec import UFS, FileStat
from ufs.utils.pathlib import SafePurePosixPath, SafePurePosixPath_

@dataclasses.dataclass
class MemoryInode:
  info: FileStat
  content: bytes = bytes()

@dataclasses.dataclass
class MemoryFileDescriptor:
  path: SafePurePosixPath_
  stream: io.BytesIO

class Memory(UFS):
  def __init__(self):
    super().__init__()
    self._inodes: dict[SafePurePosixPath_, MemoryInode] = {
      SafePurePosixPath('/'): MemoryInode({
        'type': 'directory',
        'size': 0,
      })
    }
    self._dirs: dict[SafePurePosixPath_, set[SafePurePosixPath_]] = {
      SafePurePosixPath('/'): set(),
    }
    self._cfd = iter(itertools.count(start=5))
    self._fds: dict[int, MemoryFileDescriptor] = {}

  def ls(self, path):
    try: return list(self._dirs[path])
    except KeyError: raise FileNotFoundError(path)

  def info(self, path):
    try: return self._inodes[path].info
    except KeyError: raise FileNotFoundError(path)

  def open(self, path, mode, *, size_hint = None):
    if mode.startswith('r') and path not in self._inodes: raise FileNotFoundError(path)
    if path in self._inodes and self._inodes[path].info['type'] == 'directory': raise IsADirectoryError(path)
    if path.parent not in self._dirs: raise FileNotFoundError(path.parent)
    if path not in self._inodes:
      self._inodes[path] = MemoryInode({ 'type': 'file', 'size': 0, 'atime': time.time(), 'ctime': time.time(), 'mtime': time.time(), })
      self._dirs[path.parent].add(path.name)
    fd = next(self._cfd)
    self._fds[fd] = MemoryFileDescriptor(path, io.BytesIO(self._inodes[path].content))
    if mode.startswith('a'): self._fds[fd].stream.seek(0, 2)
    return fd

  def seek(self, fd, pos, whence = 0):
    return self._fds[fd].stream.seek(pos, whence)
  def read(self, fd, amnt = -1):
    return self._fds[fd].stream.read(amnt)
  def write(self, fd, data: bytes):
    ret = self._fds[fd].stream.write(data)
    self._inodes[self._fds[fd].path].info['size'] = len(self._fds[fd].stream.getvalue())
    return ret
  def truncate(self, fd, length):
    ret = self._fds[fd].stream.truncate(length)
    self._inodes[self._fds[fd].path].info['size'] = len(self._fds[fd].stream.getvalue())
    return ret

  def close(self, fd):
    descriptor = self._fds.pop(fd)
    self._inodes[descriptor.path].content = descriptor.stream.getvalue()
    self._inodes[descriptor.path].info['size'] = len(self._inodes[descriptor.path].content)

  def unlink(self, path):
    if path not in self._inodes: raise FileNotFoundError(path)
    elif self._inodes[path].info['type'] == 'directory': raise IsADirectoryError(path)
    else:
      self._dirs[path.parent].remove(path.name)
      del self._inodes[path]

  def mkdir(self, path):
    if path in self._inodes: raise FileExistsError(path)
    if path.parent not in self._dirs: raise FileNotFoundError(path.parent)
    self._inodes[path] = MemoryInode({ 'type': 'directory', 'size': 0 })
    self._dirs[path] = set()
    self._dirs[path.parent].add(path.name)

  def rmdir(self, path):
    if path not in self._inodes: raise FileNotFoundError(path)
    if path not in self._dirs: raise NotADirectoryError(path)
    if self._dirs[path]: raise RuntimeError('Directory not Empty')
    self._dirs[path.parent].remove(path.name)
    del self._dirs[path]
    del self._inodes[path]

  def copy(self, src, dst):
    if src not in self._inodes: raise FileNotFoundError(src)
    if self._inodes[src].info['type'] == 'directory': raise IsADirectoryError(src)
    if dst in self._inodes: raise FileExistsError()
    if dst.parent not in self._dirs: raise FileNotFoundError(dst.parent)
    self._inodes[dst] = self._inodes[src]
    self._dirs[dst.parent].add(dst.name)
