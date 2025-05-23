''' An s3 filesystem implementation.

This is based off of s3fs from the fsspec ecosystem, but with several patches to successfully pass our test suite.
'''

import time
import errno
import typing as t
from ufs.spec import UFS, FileStat
from ufs.utils.pathlib import SafePurePosixPath_
from ufs.impl.fsspec import FSSpec
from s3fs import S3FileSystem
from s3fs.core import S3FileSystem
class S3(FSSpec, UFS):
  ''' FSSpec's S3 has some quirks we'll want to deal with
  '''
  def __init__(self, anon = False, access_key = None, secret_access_key = None, endpoint_url='https://s3.amazonaws.com'):
    self._anon = anon
    self._access_key = access_key
    self._secret_access_key = secret_access_key
    self._endpoint_url = endpoint_url
    self._blankfiles: dict[SafePurePosixPath_, FileStat] = {}

    super().__init__(S3FileSystem(anon=anon, key=access_key, secret=secret_access_key,
                          client_kwargs=dict(endpoint_url=endpoint_url)), ttl=0)
  
  @staticmethod
  def from_dict(*, anon, access_key, secret_access_key, endpoint_url):
    return S3(
      anon=anon,
      access_key=access_key,
      secret_access_key=secret_access_key,
      endpoint_url=endpoint_url,
    )

  def to_dict(self):
    return dict(UFS.to_dict(self),
      anon=self._anon,
      access_key=self._access_key,
      secret_access_key=self._secret_access_key,
      endpoint_url=self._endpoint_url,
    )

  def open(self, path, mode, *, size_hint: t.Optional[int] = None):
    ''' s3fs doesn't seem to throw FileNotFound when opening non-existing files for reading.
    '''
    if 'r' in mode:
      self.info(path)
    elif 'w' in mode:
      self._blankfiles[path] = {
        'type': 'file',
        'size': 0,
        'atime': time.time(),
        'ctime': time.time(),
        'mtime': time.time(),
      }
    return FSSpec.open(self, path, mode, size_hint=size_hint)

  def write(self, fd, data):
    ret = super().write(fd, data)
    path, _ = self._fds[fd]
    if path in self._blankfiles:
      self._blankfiles[path]['size'] += ret
    return ret

  def close(self, fd):
    path, _fh = self._fds[fd]
    FSSpec.close(self, fd)
    self._blankfiles.pop(path, None)

  def ls(self, path):
    files = (
      [name for name in FSSpec.ls(self, path) if name != '_']
      + [p.name for p in self._blankfiles if p.parent == path]
    )
    return files

  def info(self, path) -> FileStat:
    if path in self._blankfiles:
       return self._blankfiles[path]
    if str(path).count('/') > 1:
      try:
        info = {**FSSpec.info(self, path/'_')}
        info['type']='directory'
        return info
      except FileNotFoundError: pass
    else:
      return {
        'type': 'directory',
        'size': 0,
        'atime': 0,
        'ctime': 0,
        'mtime': 0,
      }
    return FSSpec.info(self, path)

  def mkdir(self, path):
    try: FSSpec.info(self, path)
    except FileNotFoundError as e: pass
    else: raise FileExistsError(str(path))
    if str(path).count('/') == 1: # bucket level
      FSSpec.mkdir(self, path)
    else:
      try: FSSpec.info(self, path.parent/'_')
      except FileNotFoundError as e: raise NotADirectoryError(str(path.parent)) from e
    FSSpec.put(self, path/'_', iter([b'']), size_hint=0)

  def rmdir(self, path):
    if set(FSSpec.ls(self, path)) > {'_'}:
      raise OSError(errno.ENOTEMPTY)
    try: FSSpec.unlink(self, path/'_')
    except FileNotFoundError as e: raise NotADirectoryError(str(path)) from e
    if str(path).count('/') == 1: # bucket level
      FSSpec.rmdir(self, path)

  def rename(self, src, dst):
    ''' s3fs doesn't support rename, use the fallback
    '''
    self._ls_cache.discard(self._path(dst.parent))
    return UFS.rename(self, src, dst)
