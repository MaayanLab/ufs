''' An s3 filesystem implementation.

This uses the minio python client.
'''

import io
import minio
import minio.commonconfig
from ufs.spec import UFS, DescriptorFromAtomicMixin, ReadableIterator

class S3(DescriptorFromAtomicMixin, UFS):
  def __init__(self, anon = False, access_key = None, secret_access_key = None, endpoint_url='https://s3.amazonaws.com', region=None):
    super().__init__()
    self._anon = anon
    self._access_key = access_key
    self._secret_access_key = secret_access_key
    self._endpoint_url = endpoint_url
    self._region = region

  def start(self):
    scheme, _, endpoint = self._endpoint_url.partition('://')
    assert scheme in ('http', 'https')
    self._client = minio.Minio(
      endpoint=endpoint,
      access_key=self._access_key,
      secret_key=self._secret_access_key,
      secure=scheme == 'https',
      region=self._region,
    )

  @staticmethod
  def from_dict(*, anon, access_key, secret_access_key, endpoint_url, region):
    return S3(
      anon=anon,
      access_key=access_key,
      secret_access_key=secret_access_key,
      endpoint_url=endpoint_url,
      region=region,
    )

  def to_dict(self):
    return dict(UFS.to_dict(self),
      anon=self._anon,
      access_key=self._access_key,
      secret_access_key=self._secret_access_key,
      endpoint_url=self._endpoint_url,
      region=self._region,
    )

  def ls(self, path):
    n_parts = len(path.parts)
    if n_parts == 1:
      # /
      return [
        bucket.name
        for bucket in self._client.list_buckets()
      ]
    elif n_parts == 2:
      # /{bucket}
      _, bucket = path.parts
      try:
        return [
          obj.object_name.rstrip('/')
          for obj in self._client.list_objects(bucket, recursive=False)
        ]
      except minio.error.S3Error as e:
        if e.code == 'NoSuchBucket':
          raise FileNotFoundError(path)
    elif n_parts >= 3:
      # /{bucket}/{object_name}
      _, bucket, *prefix = path.parts
      directory_name = '/'.join(prefix) + '/'
      listing = []
      exists = False
      try:
        for obj in self._client.list_objects(bucket, prefix=directory_name, recursive=False):
          object_name = obj.object_name[len(directory_name):]
          if object_name == '': exists = True
          else: listing.append(object_name.rstrip('/'))
        if not listing and not exists:
          raise FileNotFoundError(path)
      except minio.error.S3Error as e:
        if e.code == 'NoSuchBucket':
          raise FileNotFoundError(path)
      return listing

  def info(self, path):
    n_parts = len(path.parts)
    if n_parts == 1:
      # /
      return { 'type': 'directory', 'size': 0 }
    elif n_parts == 2:
      # /{bucket}
      _, bucket = path.parts
      if not self._client.bucket_exists(bucket):
        raise FileNotFoundError(path)
      return { 'type': 'directory', 'size': 0 }
    elif n_parts >= 3:
      # /{bucket}/{object_name}
      _, bucket, *object_parts = path.parts
      object_name = '/'.join(object_parts)
      try:
        for obj in self._client.list_objects(bucket, prefix=object_name):
          if (object_name + '/') in obj.object_name:
            return { 'type': 'directory', 'size': 0 }
          elif object_name == obj.object_name:
            info = { 'type': 'file', 'size': obj.size }
            mtime = obj.last_modified
            if mtime:
              info['mtime'] = mtime.timestamp()
            return info
        raise FileNotFoundError(path)
      except minio.error.S3Error as e:
        if e.code == 'NoSuchBucket':
          raise FileNotFoundError(path)

  def cat(self, path):
    n_parts = len(path.parts)
    if n_parts < 3: raise IsADirectoryError(path)
    _, bucket, *object_parts = path.parts
    object_name = '/'.join(object_parts)
    try:
      req = self._client.get_object(bucket, object_name)
      yield from req.stream()
    except minio.error.S3Error as e:
      if e.code == 'NoSuchBucket':
        raise FileNotFoundError(path)
      if e.code == 'NoSuchKey':
        raise FileNotFoundError(path)
      raise

  def put(self, path, data, *, size_hint = None):
    _, bucket, *object_parts = path.parts
    object_name = '/'.join(object_parts)
    self._client.put_object(bucket, object_name, ReadableIterator(data), size_hint or -1, part_size=10*1024*1024)

  def unlink(self, path):
    info = self.info(path)
    if info['type'] == 'directory':
      raise IsADirectoryError(path)
    n_parts = len(path.parts)
    if n_parts <= 2:
      raise IsADirectoryError(path)
    elif n_parts >= 3:
      _, bucket, *object_parts = path.parts
      object_name = '/'.join(object_parts)
      self._client.remove_object(bucket, object_name)
  
  def mkdir(self, path):
    try:
      self.info(path)
    except FileNotFoundError:
      n_parts = len(path.parts)
      if n_parts == 1:
        raise FileExistsError(path)
      elif n_parts == 2:
        _, bucket = path.parts
        self._client.make_bucket(bucket)
      elif n_parts >= 3:
        _, bucket, *object_parts = path.parts
        object_name = '/'.join(object_parts)+'/'
        self._client.put_object(bucket, object_name, io.BytesIO(b''), 0)
    else:
      raise FileExistsError(path)

  def rmdir(self, path):
    if any(True for _ in self.ls(path)):
      raise RuntimeError('Directory not Empty')
    n_parts = len(path.parts)
    if n_parts == 1:
      raise PermissionError(path)
    elif n_parts == 2:
      _, bucket = path.parts
      self._client.remove_bucket(bucket)
    elif n_parts >= 3:
      _, bucket, *object_parts = path.parts
      object_name = '/'.join(object_parts)+'/'
      self._client.remove_object(bucket, object_name)

  def copy(self, src, dst):
    ''' Try to do server-side copy, otherwise fallback
    '''
    try:
      src_info = self.info(src)
      assert src_info['type'] == 'file'
      _, src_bucket, *src_path = src.parts
      src_object_name = '/'.join(src_path)
    except KeyboardInterrupt: raise
    except:
      raise IsADirectoryError(src)
    try:
      _, dst_bucket, *dst_path = dst.parts
      dst_object_name = '/'.join(dst_path)
    except KeyboardInterrupt: raise
    except:
      raise IsADirectoryError(dst)
    if src_bucket == dst_bucket:
      try:
        self._client.copy_object(
          dst_bucket, dst_object_name, 
          minio.commonconfig.CopySource(src_bucket, src_object_name),
        )
        return
      except KeyboardInterrupt: raise
      except: pass
    super().copy(src, dst)
