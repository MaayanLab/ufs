''' An s3 filesystem implementation.

This is based off of s3fs from the fsspec ecosystem, but with several patches to successfully pass our test suite.
'''

import time
import typing as t
from ufs.spec import UFS, FileStat
from ufs.pathlib import pathparent, pathname
from ufs.impl.fsspec import FSSpec
from s3fs import S3FileSystem
from fsspec.implementations.cached import SimpleCacheFileSystem
from s3fs.core import S3FileSystem, sync_wrapper

class SimpleCacheFileSystemEx(SimpleCacheFileSystem):
    def _open(self, path, mode="rb", **kwargs):
        '''
        FIX: mode can be 'r+' or 'a', in which case we need to get it and autocomit it on close
        '''
        from fsspec.implementations.cached import LocalTempFile, os, logger, AbstractBufferedFile, BaseCache, infer_compression, compr
        path = self._strip_protocol(path)

        if "w" in mode:
            return LocalTempFile(self, path, mode=mode)
        fn = self._check_file(path)
        if fn:
            if 'r' in mode and '+' not in mode:
                return open(fn, mode)
            else:
                return LocalTempFile(self, path, fn=fn, mode=mode)

        sha = self.hash_name(path, self.same_names)
        fn = os.path.join(self.storage[-1], sha)
        logger.debug("Copying %s to local cache" % path)
        kwargs["mode"] = mode

        self._mkcache()
        if self.compression:
            with self.fs._open(path, **kwargs) as f, open(fn, "wb") as f2:
                if isinstance(f, AbstractBufferedFile):
                    # want no type of caching if just downloading whole thing
                    f.cache = BaseCache(0, f.cache.fetcher, f.size)
                comp = (
                    infer_compression(path)
                    if self.compression == "infer"
                    else self.compression
                )
                f = compr[comp](f, mode="rb")
                data = True
                while data:
                    block = getattr(f, "blocksize", 5 * 2**20)
                    data = f.read(block)
                    f2.write(data)
        else:
            self.fs.get(path, fn)
        return self._open(path, mode)


class S3FileSystemEx(S3FileSystem):

    async def _info(self, path, bucket=None, key=None, refresh=False, version_id=None):
        '''
        FIX: info didn't work for buckets, now we check for the bucket in _lsbuckets when key is falsey
        '''
        from s3fs.core import version_id_kw, ClientError, translate_boto_error, ParamValidationError
        def _coalesce_version_id(*args):
            """Helper to coalesce a list of version_ids down to one"""
            version_ids = set(args)
            if None in version_ids:
                version_ids.remove(None)
            if len(version_ids) > 1:
                raise ValueError(
                    "Cannot coalesce version_ids where more than one are defined,"
                    " {}".format(version_ids)
                )
            elif len(version_ids) == 0:
                return None
            else:
                return version_ids.pop()

        path = self._strip_protocol(path)
        bucket, key, path_version_id = self.split_path(path)
        if version_id is not None:
            if not self.version_aware:
                raise ValueError(
                    "version_id cannot be specified if the "
                    "filesystem is not version aware"
                )
        if path in ["/", ""]:
            return {"name": path, "size": 0, "type": "directory"}
        version_id = _coalesce_version_id(path_version_id, version_id)
        if not refresh:
            out = self._ls_from_cache(path)
            if out is not None:
                if self.version_aware and version_id is not None:
                    # If cached info does not match requested version_id,
                    # fallback to calling head_object
                    out = [
                        o
                        for o in out
                        if o["name"] == path and version_id == o.get("VersionId")
                    ]
                    if out:
                        return out[0]
                else:
                    out = [o for o in out if o["name"] == path]
                    if out:
                        return out[0]
                    return {"name": path, "size": 0, "type": "directory"}
        if key:
            try:
                out = await self._call_s3(
                    "head_object",
                    self.kwargs,
                    Bucket=bucket,
                    Key=key,
                    **version_id_kw(version_id),
                    **self.req_kw,
                )
                return {
                    "ETag": out.get("ETag", ""),
                    "LastModified": out["LastModified"],
                    "size": out["ContentLength"],
                    "name": "/".join([bucket, key]),
                    "type": "file",
                    "StorageClass": out.get("StorageClass", "STANDARD"),
                    "VersionId": out.get("VersionId"),
                    "ContentType": out.get("ContentType"),
                }
            except FileNotFoundError:
                pass
            except ClientError as e:
                raise translate_boto_error(e, set_cause=False)
        else:
            for info in await self._lsbuckets():
                if info['name'] == bucket:
                  return {
                      "name": bucket,
                      "Size": 0,
                      "StorageClass": "BUCKET",
                      "size": 0,
                      "type": "directory",
                  }
        try:
            # We check to see if the path is a directory by attempting to list its
            # contexts. If anything is found, it is indeed a directory
            out = await self._call_s3(
                "list_objects_v2",
                self.kwargs,
                Bucket=bucket,
                Prefix=key.rstrip("/") + "/" if key else "",
                Delimiter="/",
                MaxKeys=1,
                **self.req_kw,
            )
            if (
                out.get("KeyCount", 0) > 0
                or out.get("Contents", [])
                or out.get("CommonPrefixes", [])
            ):
                return {
                    "name": "/".join([bucket, key]),
                    "type": "directory",
                    "size": 0,
                    "StorageClass": "DIRECTORY",
                }

            raise FileNotFoundError(path)
        except ClientError as e:
            raise translate_boto_error(e, set_cause=False)
        except ParamValidationError as e:
            raise ValueError("Failed to list path %r: %s" % (path, e))

    info = sync_wrapper(_info)

    async def _mkdir(self, path, acl="", create_parents=True, **kwargs):
        '''
        If we create an empty directory, we'll add it to `dircache`

        FIX: cache mkdir calls deeper than the bucket
        '''
        from s3fs.core import buck_acls, ClientError, translate_boto_error, ParamValidationError
        path = self._strip_protocol(path).rstrip("/")
        if not path:
            raise ValueError
        bucket, key, _ = self.split_path(path)
        if await self._exists(bucket):
            if not key:
                # requested to create bucket, but bucket already exist
                raise FileExistsError
            else:
              try:
                path_info = await self._info(path)
                if path_info['type'] == 'file':
                  raise FileExistsError
              except:
                self.dircache[path] = []
        elif not key or create_parents:
            if acl and acl not in buck_acls:
                raise ValueError("ACL not in %s", buck_acls)
            try:
                params = {"Bucket": bucket, "ACL": acl}
                region_name = kwargs.get("region_name", None) or self.client_kwargs.get(
                    "region_name", None
                )
                if region_name:
                    params["CreateBucketConfiguration"] = {
                        "LocationConstraint": region_name
                    }
                await self._call_s3("create_bucket", **params)
                self.invalidate_cache("")
                self.invalidate_cache(bucket)
            except ClientError as e:
                raise translate_boto_error(e)
            except ParamValidationError as e:
                raise ValueError("Bucket create failed %r: %s" % (bucket, e))
        else:
            # raises if bucket doesn't exist and doesn't get create flag.
            await self._ls(bucket)

    mkdir = sync_wrapper(_mkdir)

    async def _rmdir(self, path):
      ''' 
      FIX: cache rmdir calls deeper than the bucket
      '''
      from s3fs.core import botocore
      path = self._strip_protocol(path).rstrip("/")
      if '/' not in path:
        try:
            await self._call_s3("delete_bucket", Bucket=path)
        except botocore.exceptions.ClientError as e:
            if "NoSuchBucket" in str(e):
                raise FileNotFoundError(path) from e
            if "BucketNotEmpty" in str(e):
                raise OSError from e
            raise
      self.invalidate_cache(path)
      self.invalidate_cache("")

    rmdir = sync_wrapper(_rmdir)

    async def _rm_file(self, path, **kwargs):
      ''' 
      FIX: invalidate the cache when a file is removed via rm_file
      '''
      await super()._rm_file(path, **kwargs)
      self.invalidate_cache(path)

    rm_file = sync_wrapper(_rm_file)

class S3(FSSpec, UFS):
  ''' FSSpec's S3 has some quirks we'll want to deal with
  '''
  def __init__(self, access_key, secret_access_key, endpoint_url='https://s3.amazonaws.com'):
    self._access_key = access_key
    self._secret_access_key = secret_access_key
    self._endpoint_url = endpoint_url
    self._dirs: dict[str, FileStat] = {}
    self._blankfiles: dict[str, FileStat] = {}

    super().__init__(
      SimpleCacheFileSystemEx(
        fs=S3FileSystemEx(key=access_key, secret=secret_access_key,
                          client_kwargs=dict(endpoint_url=endpoint_url)),
      )
    )

  @staticmethod
  def from_dict(*, access_key, secret_access_key, endpoint_url):
    return S3(
      access_key=access_key,
      secret_access_key=secret_access_key,
      endpoint_url=endpoint_url,
    )

  def to_dict(self):
    return dict(UFS.to_dict(self),
      access_key=self._access_key,
      secret_access_key=self._secret_access_key,
      endpoint_url=self._endpoint_url,
    )

  def open(self, path: str, mode: t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']) -> int:
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
    return FSSpec.open(self, path, mode)

  def write(self, fd: int, data: bytes) -> int:
    ret = super().write(fd, data)
    path, _ = self._fds[fd]
    if path in self._blankfiles:
      self._blankfiles[path]['size'] += ret
    return ret

  def close(self, fd: int):
    path, _fh = self._fds[fd]
    FSSpec.close(self, fd)
    self._blankfiles.pop(path, None)

  def ls(self, path: str) -> list[str]:
    return (
      FSSpec.ls(self, path)
      + [pathname(p) for p in self._dirs if pathparent(p) == path]
      + [pathname(p) for p in self._blankfiles if pathparent(p) == path]
    )

  def info(self, path: str) -> FileStat:
    if path in self._blankfiles:
       return self._blankfiles[path]
    if path.count('/') >= 2 and path in self._dirs:
      return self._dirs[path]
    return FSSpec.info(self, path)

  def mkdir(self, path: str):
    if path.count('/') >= 2:
      if path in self._dirs: raise FileExistsError(path)
      self._dirs[path] = {
        'type': 'directory',
        'size': 0,
        'atime': time.time(),
        'ctime': time.time(),
        'mtime': time.time(),
      }
    return FSSpec.mkdir(self, path)

  def rmdir(self, path: str):
    if path.count('/') >= 2:
      try:
        self._dirs.pop(path)
      except KeyError:
        raise FileNotFoundError(path)
    return FSSpec.rmdir(self, path)

  def rename(self, src: str, dst: str):
    ''' s3fs doesn't support rename, use the fallback
    '''
    self._ls_cache.discard(self._ppath(dst))
    return UFS.rename(self, src, dst)
