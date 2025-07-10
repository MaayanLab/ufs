from __future__ import absolute_import
import io
import errno
import typing as t
import minio
from ufs.spec import SyncUFS, DescriptorFromAtomicMixin, FileStat, AccessScope, ReadableIterator
from ufs.utils.pathlib import SafePurePosixPath

class Minio(DescriptorFromAtomicMixin, SyncUFS):
  CHUNK_SIZE = 5242880

  def __init__(self, netloc, access_key: t.Optional[str] = None, secret_key: t.Optional[str] = None, secure: bool = True):
    super().__init__()
    self._netloc = netloc
    self._access_key = access_key
    self._secret_key = secret_key
    self._secure = secure
    self._client = minio.Minio(self._netloc, access_key=access_key, secret_key=secret_key, secure=secure)

  def scope(self):
    return AccessScope.universe

  @staticmethod
  def from_dict(*, netloc, access_key, secret_key, secure):
    return Minio(
      netloc=netloc,
      access_key=access_key,
      secret_key=secret_key,
      secure=secure,
    )

  def to_dict(self):
    return dict(SyncUFS.to_dict(self),
      netloc=self._netloc,
      access_key=self._access_key,
      secret_key=self._secret_key,
      secure=self._secure,
    )

  def ls(self, path):
    if path == SafePurePosixPath():
      return [bucket.name for bucket in self._client.list_buckets()]
    else:
      _root_marker, bucket, *prefix_parts = path.parts
      prefix = ('/'.join(prefix_parts) + '/') if prefix_parts else ''
      return [obj.object_name[len(prefix):].rstrip('/') for obj in self._client.list_objects(bucket, prefix) if obj.object_name and obj.object_name[len(prefix):] != '_']

  def info(self, path) -> FileStat:
    if path == SafePurePosixPath():
      return FileStat(type='directory', size=0, atime=None, ctime=None, mtime=None)
    elif path.parent == SafePurePosixPath():
      if not self._client.bucket_exists(path.parts[1]):
        raise FileNotFoundError(path)
      return FileStat(type='directory', size=0, atime=None, ctime=None, mtime=None)
    else:
      _root_marker, bucket, *prefix_parts = path.parts
      try:
        stat = self._client.stat_object(bucket, '/'.join(prefix_parts))
        return FileStat(type='file', size=stat.size or 0, atime=None, ctime=None, mtime=stat.last_modified.timestamp() if stat.last_modified else None)
      except minio.error.S3Error as e1:
        if e1.code != 'NoSuchKey': raise e1
        try:
          stat = self._client.stat_object(bucket, '/'.join(prefix_parts+['_']))
          return FileStat(type='directory', size=0, atime=None, ctime=None, mtime=stat.last_modified.timestamp() if stat.last_modified else None)
        except minio.error.S3Error as e2:
          if e2.code != 'NoSuchKey': raise e2
          raise FileNotFoundError(path) from e1

  def unlink(self, path):
    if path == SafePurePosixPath():
      raise OSError(errno.EISDIR)
    elif path.parent == SafePurePosixPath():
      raise OSError(errno.EISDIR)
    else:
      _root_marker, bucket, *prefix_parts = path.parts
      self._client.remove_object(bucket, '/'.join(prefix_parts))

  def mkdir(self, path):
    if path == SafePurePosixPath():
      raise FileExistsError(path)
    elif path.parent == SafePurePosixPath():
      try:
        self._client.make_bucket(path.parts[1])
      except minio.error.S3Error as e:
        if e.code == 'BucketAlreadyOwnedByYou': raise FileExistsError(path)
        else: raise e
    else:
      try:
        self.info(path)
      except FileNotFoundError:
        # a file with the same name shouldn't exist
        _root_marker, bucket, *prefix_parts = path.parts
        self._client.put_object(bucket, '/'.join(prefix_parts+['_']), io.BytesIO(b''), 0)
      else:
        raise FileExistsError(path)

  def rmdir(self, path):
    if path == SafePurePosixPath():
      raise PermissionError()
    elif path.parent == SafePurePosixPath():
      self._client.remove_bucket(path.parts[1])
    else:
      if self.ls(path):
        raise OSError(errno.ENOTEMPTY)
      _root_marker, bucket, *prefix_parts = path.parts
      self._client.remove_object(bucket, '/'.join(prefix_parts+['_']))

  def cat(self, path):
    if path == SafePurePosixPath():
      raise IsADirectoryError()
    elif path.parent == SafePurePosixPath():
      raise IsADirectoryError()
    else:
      _root_marker, bucket, *prefix_parts = path.parts
      try:
        resp = self._client.get_object(bucket, '/'.join(prefix_parts))
      except minio.error.S3Error as e:
        if e.code != 'NoSuchKey': raise e
        raise FileNotFoundError(path)
      else:
        try:
          if resp.chunked:
            yield from resp.read_chunked(self.CHUNK_SIZE)
          else:
            while True:
              buf = resp.read(self.CHUNK_SIZE)
              if not buf: break
              yield buf
        finally:
          resp.close()
          resp.release_conn()

  def put(self, path, data, *, size_hint=None):
    if path == SafePurePosixPath():
      raise IsADirectoryError()
    elif path.parent == SafePurePosixPath():
      raise IsADirectoryError()
    else:
      _root_marker, bucket, *prefix_parts = path.parts
      if size_hint is not None:
        self._client.put_object(bucket, '/'.join(prefix_parts), ReadableIterator(data), size_hint)
      else:
        self._client.put_object(bucket, '/'.join(prefix_parts), ReadableIterator(data), -1, part_size=self.CHUNK_SIZE)
