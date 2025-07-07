from ufs.spec import SyncUFS
from ufs.access.pathlib import UPath, SafePurePosixPath
from fsspec import AbstractFileSystem

class UFSfsspecFileSystem(AbstractFileSystem):
  protocol = "ufs"

  def __init__(self, ufs: SyncUFS, **kwargs):
    self.ufs = ufs
    self.upath = UPath(ufs)

  @classmethod
  def _strip_protocol(cls, path):
    path = AbstractFileSystem._strip_protocol(path)
    if isinstance(path, list):
      return [str(SafePurePosixPath(p)) for p in path]
    return str(SafePurePosixPath(path))

  def mkdir(self, path, create_parents=True, **kwargs):
    path = self._strip_protocol(path)
    if isinstance(path, list): raise NotImplementedError()
    (self.upath / path).mkdir(parents=create_parents)
  
  def makedirs(self, path, exist_ok=False):
    path = self._strip_protocol(path)
    if isinstance(path, list): raise NotImplementedError()
    (self.upath / path).mkdir(parents=True, exist_ok=exist_ok)
  
  def rmdir(self, path):
    path = self._strip_protocol(path)
    if isinstance(path, list): raise NotImplementedError()
    (self.upath / path).rmdir()
  
  def ls(self, path, detail=True, **kwargs):
    path = self._strip_protocol(path)
    if isinstance(path, list): raise NotImplementedError()
    dir_info = self.info(path)
    if dir_info['type'] == 'file':
      return [dir_info] if detail else [dir_info['name']]
    return [
      file_info if detail else file_info['name']
      for p in (self.upath / path).iterdir()
      for file_info in (self.info(p._path),)
    ]

  def info(self, path, **kwargs):
    path = self._strip_protocol(path)
    if isinstance(path, list): raise NotImplementedError()
    info = self.ufs.info((self.upath / path)._path)
    return dict(info, name=str(self.upath / path))

  def open(self, path, mode='rb', **kwargs):
    path = self._strip_protocol(path)
    if isinstance(path, list): raise NotImplementedError()
    if 'a' in mode or 'w' in mode: (self.upath / path).parent.mkdir(parents=True, exist_ok=True)
    return (self.upath / path).open(mode)

  def rm_file(self, path):
    path = self._strip_protocol(path)
    if isinstance(path, list): raise NotImplementedError()
    try:
      return (self.upath/path).unlink()
    except IsADirectoryError:
      return (self.upath/path).rmdir()

  def cp_file(self, path1, path2, **kwargs):
    path1 = self._strip_protocol(path1)
    path2 = self._strip_protocol(path2)
    if isinstance(path2, list):
      raise NotImplementedError()
    if isinstance(path1, list):
      for p1 in path1:
        self.cp_file(p1, SafePurePosixPath(path2)/SafePurePosixPath(p1).name)
    elif (self.upath/path1).is_dir():
      for p1 in (self.upath/path1).iterdir():
        self.cp_file(str(p1), SafePurePosixPath(path2)/p1.name)
    else:
      (self.upath/path2).parent.mkdir(parents=True, exist_ok=True)
      if (self.upath/path2).is_dir():
        self.ufs.copy(SafePurePosixPath(path1), SafePurePosixPath(path2)/SafePurePosixPath(path1).name)
      else:
        self.ufs.copy(SafePurePosixPath(path1), SafePurePosixPath(path2))
