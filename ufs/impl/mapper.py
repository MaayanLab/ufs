import logging
import itertools
import typing as t
from ufs.spec import UFS
from ufs.utils.pathlib import SafePurePosixPath
from ufs.utils.prefix_tree import create_prefix_tree_from_paths, search_prefix_tree, list_prefix_tree

logger = logging.getLogger(__name__)

class Mapper(UFS):
  def __init__(self, pathmap: t.Dict[str, UFS]):
    '''
    pathmap: dict
      A mapping from [path to map onto the target filesystem]: UFS
    '''
    super().__init__()
    self._pathmap = { SafePurePosixPath(k): v for k, v in pathmap.items() }
    self._prefix_tree = create_prefix_tree_from_paths(list(self._pathmap.keys()))
    self._fds = {}
    self._cfd = iter(itertools.count())

  def _matchpath(self, path):
    ''' Return (fs, path) depending on whether we hit a mapped paths or not
    '''
    prefix, subpath = search_prefix_tree(self._prefix_tree, path)
    ufs = self._pathmap.get(prefix)
    if not ufs: raise FileNotFoundError(path)
    return ufs, subpath

  @staticmethod
  def from_dict(*, pathmap):
    return Mapper(
      pathmap={
        k: UFS.from_dict(**v)
        for k, v in pathmap.items()
      },
    )

  def to_dict(self):
    return dict(super().to_dict(),
      pathmap={
        k: v.to_dict()
        for k, v in self._pathmap.items()
      },
    )

  def ls(self, path):
    prefix, prefix_subpath, prefix_listing = list_prefix_tree(self._prefix_tree, path)
    ufs = self._pathmap.get(prefix)
    if ufs:
      try:
        ufs_listing = ufs.ls(prefix_subpath)
      except FileNotFoundError:
        ufs_listing = None
    else:
      ufs_listing = None
    if prefix_listing is None and ufs_listing is None:
      raise FileNotFoundError(path)
    return list(set(ufs_listing or {}) | (prefix_listing or {}))

  def info(self, path):
    ufs, subpath = self._matchpath(path)
    return ufs.info(subpath)

  def open(self, path, mode, *, size_hint = None):
    ufs, subpath = self._matchpath(path)
    ufs_fd = ufs.open(subpath, mode, size_hint=size_hint)
    fd = next(self._cfd)
    self._fds[fd] = (ufs, ufs_fd)
    return fd

  def seek(self, fd, pos, whence = 0):
    ufs, ufs_fd = self._fds[fd]
    return ufs.seek(ufs_fd, pos, whence)

  def read(self, fd, amnt):
    ufs, ufs_fd = self._fds[fd]
    return ufs.read(ufs_fd, amnt)

  def write(self, fd, data):
    ufs, ufs_fd = self._fds[fd]
    return ufs.write(ufs_fd, data)

  def truncate(self, fd, length):
    ufs, ufs_fd = self._fds[fd]
    return ufs.truncate(ufs_fd, length)

  def flush(self, fd):
    ufs, ufs_fd = self._fds[fd]
    return ufs.flush(ufs_fd)

  def close(self, fd):
    ufs, ufs_fd = self._fds.pop(fd)
    return ufs.close(ufs_fd)

  def unlink(self, path):
    ufs, subpath = self._matchpath(path)
    return ufs.unlink(subpath)

  def mkdir(self, path):
    ufs, subpath = self._matchpath(path)
    return ufs.mkdir(subpath)

  def rmdir(self, path):
    ufs, subpath = self._matchpath(path)
    return ufs.rmdir(subpath)

  def copy(self, src, dst):
    from ufs.access.shutil import copyfile
    src_ufs, src_subpath = self._matchpath(src)
    dst_ufs, dst_subpath = self._matchpath(dst)
    copyfile(src_ufs, src_subpath, dst_ufs, dst_subpath)

  def rename(self, src, dst):
    from ufs.access.shutil import movefile
    src_ufs, src_subpath = self._matchpath(src)
    dst_ufs, dst_subpath = self._matchpath(dst)
    movefile(src_ufs, src_subpath, dst_ufs, dst_subpath)

  def start(self):
    for fs in self._pathmap.values():
      fs.start()

  def stop(self):
    for fs in self._pathmap.values():
      fs.stop()
