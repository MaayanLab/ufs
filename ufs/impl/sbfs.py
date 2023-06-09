import json
import aiohttp
import itertools
import dataclasses
import logging
from ufs.spec import AsyncUFS
from ufs.utils.pathlib import SafePurePosixPath_

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class SBFSReadFileDescriptor:
  path: SafePurePosixPath_
  download_info: dict
  pos: int = 0

@dataclasses.dataclass
class SBFSWriteFileDescriptor:
  path: SafePurePosixPath_
  upload_info: dict
  part: bytes = b''
  pos: int = 0
  part_num: int = 1

def one(items):
  it = iter(items)
  try: item = next(it)
  except StopIteration: raise
  try: next(it)
  except StopIteration: return item
  else: raise RuntimeError('Expected one, got multiple')

class SBFS(AsyncUFS):
  CHUNK_SIZE = 8192

  def __init__(self, auth_token: str, api_endpoint='https://cavatica-api.sbgenomics.com'):
    super().__init__()
    assert auth_token is not None, 'SBFS auth_token is required'
    self._auth_token = auth_token
    self._api_endpoint = api_endpoint
    self._cfd = iter(itertools.count(start=5))
    self._fds = {}

  @staticmethod
  def from_dict(*, auth_token, api_endpoint):
    return SBFS(
      auth_token=auth_token,
      api_endpoint=api_endpoint,
    )

  def to_dict(self):
    return dict(super().to_dict(),
      auth_token=self._auth_token,
      api_endpoint=self._api_endpoint,
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
    async with self._session.get(f"{self._api_endpoint}/v2/user") as req:
      return await req.json()

  async def _projects(self, params: dict):
    async with self._session.get(f"{self._api_endpoint}/v2/projects", params=params) as res:
      ret = await res.json()
    logger.info(f"{ret['items']}")
    for item in ret['items']:
      yield item

  async def _files(self, params: dict):
    async with self._session.get(f"{self._api_endpoint}/v2/files", params=params) as res:
      ret = await res.json()
    for item in ret['items']:
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
    import aiohttp.client_exceptions
    try:
      info = one([
        item
        async for item in self._files(params=dict(await self._as_parent_params(path.parent), name=path.name))
        if item['name'] == path.name
      ])
      logger.debug(f"_file_info({path}) -> {info}")
      return info
    except aiohttp.client_exceptions.ClientResponseError as e:
      if e.code == 404: raise FileNotFoundError(str(path))
      else: raise e
    except StopIteration:
      raise FileNotFoundError(str(path))

  async def _file_details(self, file_info: dict, params: dict):
    async with self._session.get(f"{self._api_endpoint}/v2/files/{file_info['id']}", params=params) as req:
      return await req.json()

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
      if not any([proj['id'].startswith(f"{user}/") async for proj in self._projects()]):
        raise FileNotFoundError(str(path))
      return { 'type': 'directory', 'size': 0 }
    elif n_parts == 3:
      # /{user}/{proj}
      _, user, name = path.parts
      if not any([proj['id'] == f"{user}/{name}" async for proj in self._projects(params=dict(name=name))]):
        raise FileNotFoundError(str(path))
      return { 'type': 'directory', 'size': 0 }
    elif n_parts >= 4:
      # /{user}/{proj}/**
      info = await self._file_info(path)
      if info['type'] == 'folder':
        return { 'type': 'directory', 'size': 0 }
      elif info['type'] == 'file':
        details = await self._file_details(info, params=dict(fields='size,created_on,modified_on'))
        logger.info(f"{details=}")
        # TODO: capture, atime, mtime, e.g. 2023-06-09T14:01:56Z
        return { 'type': 'file', 'size': details['size'] }
      else:
        raise NotImplementedError(info['type'])

  async def open(self, path, mode, *, size_hint = None):
    if '+' in mode:
      raise NotImplementedError(mode)
    if 'r' in mode:
      file_info = await self._file_info(path)
      file_details = await self._file_details(file_info, params=dict(fields='size'))
      async with self._session.get(f"{self._api_endpoint}/v2/files/{file_info['id']}/download_info") as req:
        download_info = await req.json()
      descriptor = SBFSReadFileDescriptor(path, dict(download_info, size=file_details['size']))
      fd = next(self._cfd)
      self._fds[fd] = descriptor
    elif 'w' in mode:
      if not size_hint: raise NotImplementedError('no sizehint write')
      async with self._session.post(f"{self._api_endpoint}/v2/upload/multipart", params=dict(overwrite='true'), json=dict(
        await self._as_parent_params(path.parent),
        name=path.name,
        size=size_hint,
        part_size=min(1073741824, size_hint),
      )) as req:
        upload_info = await req.json()
      descriptor = SBFSWriteFileDescriptor(path, upload_info)
      fd = next(self._cfd)
      self._fds[fd] = descriptor
    else:
      raise NotImplementedError(mode)
    return fd
  
  async def seek(self, fd, pos, whence = 0):
    if whence != 0: raise NotImplementedError()
    if isinstance(self._fds[fd], SBFSWriteFileDescriptor) and pos != self._fds[fd].pos:
      raise NotImplementedError()
    self._fds[fd].pos = pos

  async def read(self, fd, amnt = -1):
    descriptor = self._fds[fd]
    if isinstance(descriptor, SBFSWriteFileDescriptor):
      raise RuntimeError()
    if amnt == 0:
      ret = b''
    elif amnt > 0:
      start = descriptor.pos
      end = min(descriptor.download_info['size'], descriptor.pos + amnt)
      if start == end:
        ret = b''
      else:
        logger.debug(f"{descriptor.download_info['url']} bytes={start}-{end}")
        async with self._session.get(descriptor.download_info['url'], headers={
          'Range': f"bytes={start}-{end}"
        }) as req:
          ret = await req.content.read()
          descriptor.pos += len(ret)
    else:
      async with self._session.get(descriptor.download_info['url']) as req:
        ret = await req.content.read()
        descriptor.pos += len(ret)
    return ret

  async def write(self, fd, data):
    descriptor = self._fds[fd]
    if isinstance(descriptor, SBFSReadFileDescriptor):
      raise RuntimeError()
    descriptor.part += data
    return len(data)

  async def truncate(self, fd, length):
    raise NotImplementedError()

  async def flush(self, fd: int):
    descriptor = self._fds[fd]
    if isinstance(descriptor, SBFSWriteFileDescriptor):
      while descriptor.part and (len(descriptor.part) >= descriptor.upload_info['part_size'] or descriptor.pos + len(descriptor.part) >= descriptor.upload_info['size']):
        part = descriptor.part[:descriptor.upload_info['part_size']]
        async with self._session.get(f"{self._api_endpoint}/v2/upload/multipart/{descriptor.upload_info['upload_id']}/part/{descriptor.part_num}") as req:
          part_info = await req.json()
          logger.debug(part_info)
        async with self._session.request(part_info['method'], part_info['url'], data=part, headers=part_info['headers']) as req:
          logger.debug(await req.read())
          upload_response = {'headers': {k: json.loads(req.headers.get(k)) for k in part_info['report']['headers']}}
        async with self._session.post(f"{self._api_endpoint}/v2/upload/multipart/{descriptor.upload_info['upload_id']}/part", json=dict(
          part_number=descriptor.part_num,
          response=upload_response,
        )) as req:
          logger.debug(await req.read())
        descriptor.pos += len(part)
        descriptor.part = descriptor.part[descriptor.pos:]
        descriptor.part_num += 1

  async def close(self, fd):
    await self.flush(fd)
    descriptor = self._fds.pop(fd)
    if isinstance(descriptor, SBFSWriteFileDescriptor):
      logger.debug(f"{descriptor=}")
      if descriptor.pos == descriptor.upload_info['size']:
        logger.debug(f"complete")
        async with self._session.post(f"{self._api_endpoint}/v2/upload/multipart/{descriptor.upload_info['upload_id']}/complete") as req:
          logger.debug(await req.read())
      else:
        logger.debug(f"cancel")
        async with self._session.delete(f"{self._api_endpoint}/v2/upload/multipart/{descriptor.upload_info['upload_id']}") as req:
          logger.debug(await req.read())

  async def unlink(self, path):
    info = await self._file_info(path)
    if info['type'] == 'directory':
      raise IsADirectoryError(str(path))
    await self._delete(info)

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
      async with self._session.post(f"{self._api_endpoint}/v2/files", json=dict(
        await self._as_parent_params(path.parent),
        name=path.name,
        type='folder',
      )) as req:
        logger.debug(await req.read())

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
      logger.debug(f"{info=}")
      if info['type'] != 'folder':
        raise NotADirectoryError(path)
      await self._delete(info)

  async def copy(self, src, dst):
    src_info = await self._file_info(src)
    async with self._session.post(f"{self._api_endpoint}/v2/files/{src_info['id']}/actions/copy", json=dict(
      await self._as_parent_params(dst.parent),
      name=dst.name,
    )) as req:
      logger.debug(await req.read())

  async def rename(self, src, dst):
    src_info = await self._file_info(src)
    async with self._session.post(f"{self._api_endpoint}/v2/files/{src_info['id']}/actions/move", json=dict(
      await self._as_parent_params(dst.parent),
      name=dst.name,
    )) as req:
      logger.debug(await req.read())
