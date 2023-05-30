import typing as t
from ufs.spec import UFS
from ufs.impl.fsspec import FSSpec
from s3fs import S3FileSystem
from fsspec.implementations.cached import SimpleCacheFileSystem

class S3(FSSpec):
  ''' FSSpec's S3 has some quirks we'll want to deal with
  '''
  def __init__(self, access_key, secret_access_key, endpoint_url='https://s3.amazonaws.com'):
    super().__init__(SimpleCacheFileSystem(
      fs=S3FileSystem(key=access_key, secret=secret_access_key, endpoint_url=endpoint_url)
    ))

  def open(self, path: str, mode: t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']) -> int:
    ''' s3fs doesn't seem to throw FileNotFound when opening non-existing files for reading.
    '''
    if 'r' in mode:
      self.info(path)
    return super().open(path, mode)

  def rmdir(self, path: str):
    ''' s3fs rmdir for directories that aren't a bucket is broken
    '''
    self._ls_cache.discard(self._path(path))
    self._ls_cache.discard(self._ppath(path))
    if '/' not in path[1:]:
      self._fs.rmdir(self._path(path))
    self._fs.invalidate_cache(self._path(path))
    self._fs.invalidate_cache("")
  
  def rename(self, src: str, dst: str):
    ''' s3fs doesn't support rename, use the fallback
    '''
    self._ls_cache.discard(self._ppath(dst))
    return UFS.rename(self, src, dst)
