''' The UFS generic interface for universal filesystem implementation
'''
import typing as t

class FileStat(t.TypedDict):
  type: t.Literal['file', 'directory']
  size: int
  atime: t.Optional[float]
  ctime: t.Optional[float]
  mtime: t.Optional[float]

class UFS:
  ''' A generic class interface for universal file system implementations
  '''
  @staticmethod
  def from_dict(*, cls, **kwargs):
    import importlib
    mod, _, name = cls.rpartition('.')
    cls = getattr(importlib.import_module(mod), name)
    if cls.from_dict is UFS.from_dict: return cls(**kwargs)
    else: return cls.from_dict(**kwargs)

  def to_dict(self):
    cls = self.__class__
    return dict(cls=f"{cls.__module__}.{cls.__name__}")

  # essential
  def ls(self, path: str) -> list[str]:
    raise NotImplementedError()
  def info(self, path: str) -> FileStat:
    raise NotImplementedError()
  def open(self, path: str, mode: t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']) -> int:
    raise NotImplementedError()
  def seek(self, fd: int, pos: int, whence: t.Literal[0, 1, 2] = 0):
    raise NotImplementedError()
  def read(self, fd: int, amnt: int) -> bytes:
    raise NotImplementedError()
  def write(self, fd: int, data: bytes) -> int:
    raise NotImplementedError()
  def truncate(self, fd: int, length: int):
    raise NotImplementedError()
  def close(self, fd: int):
    raise NotImplementedError()
  def unlink(self, path: str):
    raise NotImplementedError()

  # optional
  def mkdir(self, path: str):
    pass
  def rmdir(self, path: str):
    pass
  def flush(self, fd: int):
    pass
  def start(self):
    pass
  def stop(self):
    pass

  # fallback
  def copy(self, src: str, dst: str):
    src_fd = self.open(src, 'r')
    dst_fd = self.open(dst, 'w')
    while buf := self.read(src_fd, 5*1024):
      self.write(dst_fd, buf)
    self.close(dst)
    self.close(src)

  def rename(self, src: str, dst: str):
    self.copy(src, dst)
    self.unlink(src)

  def __repr__(self) -> str:
    return f"UFS({self.__class__.__name__})"
