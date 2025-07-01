''' Seven Bridges File System UFS
'''
import json
import aiohttp
import logging
from datetime import datetime
from ufs.spec import AsyncUFS, AsyncDescriptorFromAtomicMixin, AccessScope
from ufs.utils.pathlib import SafePurePosixPath_
from ufs.utils.cache import TTLCacheStore, Result
from ufs.utils.one import one

logger = logging.getLogger(__name__)

class SBFS(AsyncDescriptorFromAtomicMixin, AsyncUFS):
  CHUNK_SIZE = 8192

  def __init__(self, auth_token: str, api_endpoint='https://cavatica-api.sbgenomics.com', drs_endpoint='drs://cavatica-ga4gh-api.sbgenomics.com', ttl=60):
    super().__init__()
    assert auth_token, 'SBFS auth_token is required'
    self._auth_token = auth_token
    self._api_endpoint = api_endpoint
    self._drs_endpoint = drs_endpoint
    self._ttl = ttl
    self._sbfs_cache = TTLCacheStore(ttl=self._ttl)

  def scope(self):
    return AccessScope.universe

  @staticmethod
  def from_dict(*, auth_token, api_endpoint, drs_endpoint, ttl):
    return SBFS(
      auth_token=auth_token,
      api_endpoint=api_endpoint,
      drs_endpoint=drs_endpoint,
      ttl=ttl,
    )

  def to_dict(self):
    return dict(super().to_dict(),
      auth_token=self._auth_token,
      api_endpoint=self._api_endpoint,
      drs_endpoint=self._drs_endpoint,
      ttl=self._ttl,
    )

  async def start(self):
    self._session_mgr = aiohttp.ClientSession(
      headers={
        'X-SBG-Auth-Token': self._auth_token,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
      raise_for_status=True,
    )
    self._session = await self._session_mgr.__aenter__()

  async def _user(self):
    try:
      user = self._sbfs_cache['user:']
    except KeyError:
      async with self._session.get(f"{self._api_endpoint}/v2/user") as req:
         self._sbfs_cache['user:'] = user = await req.json()
    return user

  async def _projects(self, params: dict = {}):
    # TODO: paginate
    try:
      items = self._sbfs_cache['projects:'+str(params)]
    except KeyError:
      async with self._session.get(f"{self._api_endpoint}/v2/projects", params=params) as res:
        ret = await res.json()
      self._sbfs_cache['projects:'+str(params)] = items = ret['items']
    for item in items:
      yield item

  async def _files(self, params: dict):
    # TODO: paginate
    try:
      items = self._sbfs_cache['files:'+str(params)]
    except KeyError:
      async with self._session.get(f"{self._api_endpoint}/v2/files", params=params) as res:
        ret = await res.json()
      self._sbfs_cache['files:'+str(params)] = items = ret['items']
    for item in items:
      yield item

  async def _as_parent_params(self, path: SafePurePosixPath_):
    if len(path.parts) < 3:
      raise PermissionError(str(path))
    elif len(path.parts) == 3:
      _, user, project = path.parts
      return dict(project=f"{user}/{project}")
    else:
      return dict(parent=(await self._file_info(path))['id'])
    
  async def _file_info(self, path: SafePurePosixPath_):
    try:
      entry = self._sbfs_cache['file_info:'+str(path)]
    except KeyError:
      import aiohttp.client_exceptions
      try:
        info = one([
          item
          async for item in self._files(params=dict(await self._as_parent_params(path.parent), name=path.name))
          if item['name'] == path.name
        ])
        logger.debug(f"_file_info({path}) -> {info}")
        entry = Result(val=info)
      except aiohttp.client_exceptions.ClientResponseError as e:
        if e.code == 404: entry = Result(err=FileNotFoundError(str(path)))
        else: entry = Result(err=e)
      except StopIteration:
        entry = Result(err=FileNotFoundError(str(path)))
      self._sbfs_cache['file_info:'+str(path)] = entry
    if entry.err is not None: raise entry.err
    else: return entry.val

  async def _file_details(self, file_info: dict):
    try:
      ret = self._sbfs_cache['file_details:'+file_info['id']]
    except KeyError:
      async with self._session.get(f"{self._api_endpoint}/v2/files/{file_info['id']}", params=dict(fields='size,created_on,modified_on')) as req:
        self._sbfs_cache['file_details:'+file_info['id']] = ret = await req.json()
    return ret

  async def _delete(self, file_info):
    async with self._session.delete(f"{self._api_endpoint}/v2/files/{file_info['id']}") as res:
      return await res.text()

  async def ls(self, path):
    n_parts = len(path.parts)
    if n_parts == 1:
      # /
      return list({
        item_user
        async for item in self._projects()
        for item_user, _item_proj in (item['id'].split('/'),)
      })
    elif n_parts == 2:
      # /{user}
      _, user = path.parts
      return [
        item_proj
        async for item in self._projects()
        for item_user, item_proj in (item['id'].split('/'),)
        if user == item_user
      ]
    elif n_parts >= 3:
      # /{user}/{proj}/{dir...}
      return [
        item['name']
        async for item in self._files(params=await self._as_parent_params(path))
      ]

  async def info(self, path):
    n_parts = len(path.parts)
    if n_parts == 1:
      # /
      return { 'type': 'directory', 'size': 0 }
    elif n_parts == 2:
      # /{user}
      _, user = path.parts
      if not any([
        item_user == user
        async for item in self._projects()
        for item_user, _item_proj in (item['id'].split('/'),)
      ]):
        raise FileNotFoundError(str(path))
      return { 'type': 'directory', 'size': 0 }
    elif n_parts == 3:
      # /{user}/{proj}
      _, user, name = path.parts
      if not any([
        item_user == user and item_proj == name
        async for item in self._projects()
        for item_user, item_proj in (item['id'].split('/'),)
      ]):
        raise FileNotFoundError(str(path))
      return { 'type': 'directory', 'size': 0 }
    elif n_parts >= 4:
      # /{user}/{proj}/**
      info = await self._file_info(path)
      if info['type'] == 'folder':
        return { 'type': 'directory', 'size': 0 }
      elif info['type'] == 'file':
        details = await self._file_details(info)
        logger.info(f"{details}")
        ret = { 'type': 'file', 'size': details['size'] }
        if self._drs_endpoint:
          ret['drs'] = f"{self._drs_endpoint}/{info['id']}"
        try: ret['ctime'] = datetime.fromisoformat(details['created_on']).timestamp()
        except: pass
        try: ret['mtime'] = datetime.fromisoformat(details['modified_on']).timestamp()
        except: pass
        return ret
      else:
        raise NotImplementedError(info['type'])

  async def cat(self, path):
    file_info = await self._file_info(path)
    async with self._session.get(f"{self._api_endpoint}/v2/files/{file_info['id']}/download_info") as req:
      download_info = await req.json()
    async with self._session.get(download_info['url']) as req:
      async for chunk, _ in req.content.iter_chunks():
        yield chunk

  async def put(self, path, data, *, size_hint=None):
    if not size_hint: raise NotImplementedError('no sizehint write')
    async with self._session.post(f"{self._api_endpoint}/v2/upload/multipart", params=dict(overwrite='true'), json=dict(
      await self._as_parent_params(path.parent),
      name=path.name,
      size=size_hint,
      part_size=min(1073741824, size_hint),
    )) as req:
      upload_info = await req.json()
    try:
      part = b''
      pos = 0
      part_number = 1

      async for buf in data:
        part += buf

        while part and (len(part) >= upload_info['part_size'] or pos + len(part) >= size_hint):
          part = part[:upload_info['part_size']]
          async with self._session.get(f"{self._api_endpoint}/v2/upload/multipart/{upload_info['upload_id']}/part/{part_number}") as req:
            part_info = await req.json()
            logger.debug(part_info)
          async with self._session.request(part_info['method'], part_info['url'], data=part, headers=part_info['headers']) as req:
            logger.debug(await req.read())
            upload_response = {'headers': {k: json.loads(req.headers.get(k)) for k in part_info['report']['headers']}}
          async with self._session.post(f"{self._api_endpoint}/v2/upload/multipart/{upload_info['upload_id']}/part", json=dict(
            part_number=part_number,
            response=upload_response,
          )) as req:
            logger.debug(await req.read())
          pos += len(part)
          part = part[pos:]
          part_number += 1

      assert pos == size_hint, f"{pos} != {size_hint}"
    except:
      logger.debug(f"cancel")
      async with self._session.delete(f"{self._api_endpoint}/v2/upload/multipart/{upload_info['upload_id']}") as req:
        logger.debug(await req.read())
    else:
      logger.debug(f"complete")
      async with self._session.post(f"{self._api_endpoint}/v2/upload/multipart/{upload_info['upload_id']}/complete") as req:
        logger.debug(await req.read())
    self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(path.parent), name=path.name)))
    self._sbfs_cache.discard('file_info:'+str(path))

  async def unlink(self, path):
    info = await self._file_info(path)
    if info['type'] == 'directory':
      raise IsADirectoryError(str(path))
    await self._delete(info)
    parent_params = await self._as_parent_params(path.parent)
    # parent directory listing
    self._sbfs_cache.discard('files:'+str(dict(parent_params)))
    # file in parent directory listing
    self._sbfs_cache.discard('files:'+str(dict(parent_params, name=path.name)))
    self._sbfs_cache.discard('file_info:'+str(path))
    self._sbfs_cache.discard('file_details:'+str(info['id']))

  async def stop(self):
    await self._session_mgr.__aexit__(None, None, None)

  async def mkdir(self, path):
    n_parts = len(path.parts)
    if n_parts == 1: raise FileExistsError(str(path))
    elif n_parts == 2: raise PermissionError(str(path))
    elif n_parts == 3:
      _, user, proj = path.parts
      if user != (await self._user())['username']:
        raise PermissionError(str(path))
      try:
        info = await self.info(path)
        raise FileExistsError(str(path))
      except FileNotFoundError:
        pass
      async with self._session.post(f"{self._api_endpoint}/v2/projects", json=dict(
        name=proj,
      )) as req:
        logger.debug(await req.read())
    elif n_parts >= 4:
      try:
        info = await self.info(path)
        raise FileExistsError(str(path))
      except FileNotFoundError:
        pass
      async with self._session.post(f"{self._api_endpoint}/v2/files", json=dict(
        await self._as_parent_params(path.parent),
        name=path.name,
        type='folder',
      )) as req:
        self._sbfs_cache['file_info:'+str(path)] = Result(val=await req.json())
    self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(path.parent))))
    self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(path.parent), name=path.name)))

  async def rmdir(self, path):
    n_parts = len(path.parts)
    if n_parts <= 2:
      raise PermissionError(str(path))
    elif n_parts == 3:
      if await self.ls(path):
        import os, errno
        raise OSError(errno.ENOTEMPTY, os.strerror(errno.ENOTEMPTY), str(path))
      _, user, proj = path.parts
      async with self._session.delete(f"{self._api_endpoint}/v2/projects/{user}/{proj}") as req:
        logger.debug(await req.read())
    else:
      info = await self._file_info(path)
      logger.debug(f"{info}")
      if info['type'] != 'folder':
        raise NotADirectoryError(path)
      await self._delete(info)
      self._sbfs_cache.discard('file_info:'+str(path))
      self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(path.parent))))
      self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(path.parent), name=path.name)))
      self._sbfs_cache.discard('file_details:'+str(info['id']))

  async def copy(self, src, dst):
    src_info = await self._file_info(src)
    async with self._session.post(f"{self._api_endpoint}/v2/files/{src_info['id']}/actions/copy", json=dict(
      await self._as_parent_params(dst.parent),
      name=dst.name,
    )) as req:
      logger.debug(await req.read())
    self._sbfs_cache.discard('file_info:'+str(dst))
    self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(dst.parent))))
    self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(dst.parent), name=dst.name)))

  async def rename(self, src, dst):
    src_info = await self._file_info(src)
    async with self._session.post(f"{self._api_endpoint}/v2/files/{src_info['id']}/actions/move", json=dict(
      await self._as_parent_params(dst.parent),
      name=dst.name,
    )) as req:
      logger.debug(await req.read())
    self._sbfs_cache.discard('file_info:'+str(src))
    self._sbfs_cache.discard('file_info:'+str(dst))
    self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(src.parent))))
    self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(src.parent), name=src.name)))
    self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(dst.parent))))
    self._sbfs_cache.discard('files:'+str(dict(await self._as_parent_params(dst.parent), name=dst.name)))
