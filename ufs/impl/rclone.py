''' Access any `rclone`-supported protocol
'''

import uuid
import json
import logging
import requests
import functools
import contextlib
import typing as t
from ufs.spec import SyncUFS, DescriptorFromAtomicMixin, ReadableIterator, AccessScope
from ufs.utils.pathlib import SafePurePosixPath

logger = logging.getLogger(__name__)

def rclone_uri_from_path(path):
  if path == SafePurePosixPath():
    return None, None
  _, fs, *parts = path.parts
  if not parts:
    return fs+':', None
  return fs+':', SafePurePosixPath('/'.join(parts))

def rstrip_iter(it, rstrip):
  last = None
  for el in it:
    if last is not None:
      yield last
    last = el
  yield last.rstrip(rstrip)

@contextlib.contextmanager
def serve_rclone_rcd(env: dict = {}):
  ''' RClone operates through an `rclone rcd` server, this helper
  can be used to ensure one is running for the duration of the context manager.
  '''
  import sys
  import socket
  import shutil
  from subprocess import Popen
  from ufs.utils.process import active_process
  from ufs.utils.polling import wait_for, safe_predicate
  rclone = shutil.which('rclone')
  docker = shutil.which('docker')
  assert rclone or docker, 'Failed find binary for running rclone'
  with socket.socket() as s:
    s.bind(('127.0.0.1', 0))
    rclone_host, rclone_port = s.getsockname()
  rclone_user, rclone_pass = str(uuid.uuid4()), str(uuid.uuid4())
  if rclone:
    proc = Popen([
      rclone, 'rcd',
      '--rc-addr', f"{rclone_host}:{rclone_port}",
      '--rc-user', rclone_user,
      '--rc-pass', rclone_pass,
    ], env=env, stdout=sys.stdout, stderr=sys.stderr)
  else:
    proc = Popen([
      docker, 'run',
      *[arg for key in env for arg in ['-e', key]],
      '-p', f"{rclone_port}:8080",
      '-i', 'rclone/rclone', 'rcd',
      '--rc-addr', f"0.0.0.0:8080",
      '--rc-user', rclone_user,
      '--rc-pass', rclone_pass,
    ], stdout=sys.stdout, stderr=sys.stderr)
  with active_process(proc):
    rclone_config = dict(url=f"http://{rclone_host}:{rclone_port}", auth=(rclone_user, rclone_pass))
    wait_for(functools.partial(safe_predicate, lambda: requests.post(f"{rclone_config['url']}/config/listremotes", auth=rclone_config['auth']).status_code == 200))
    yield rclone_config

class RClone(DescriptorFromAtomicMixin, SyncUFS):
  def __init__(self, url: str, auth: t.Tuple[str, str]):
    super().__init__()
    self._url = url
    self._auth = auth

  def scope(self):
    return AccessScope.system

  @staticmethod
  def from_dict(*, url, auth):
    return RClone(
      url=url,
      auth=auth,
    )

  def to_dict(self):
    return dict(super().to_dict(),
      url=self._url,
      auth=self._auth,
    )

  def ls(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs:
      req = requests.post(
        f"{self._url}/config/listremotes",
        auth=self._auth,
      )
      if req.status_code != 200:
        raise FileNotFoundError()
      ret = req.json()
      return ret['remotes']
    else:
      if not path: path = ''
      req = requests.post(
        f"{self._url}/operations/list",
        auth=self._auth,
        params=dict(fs=fs, remote=str(path)[1:]),
      )
      if req.status_code != 200:
        raise FileNotFoundError()
      ret = req.json()
      return [item['Name'] for item in ret['list']]

  def info(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs:
      return { 'type': 'directory', 'size': 0 }
    if not path:
      req = requests.post(
        f"{self._url}/operations/fsinfo",
        auth=self._auth,
        params=dict(fs=fs),
      )
      if req.status_code != 200:
        raise FileNotFoundError
      return { 'type': 'directory', 'size': 0 }
    else:
      req = requests.post(
        f"{self._url}/operations/list",
        auth=self._auth,
        params=dict(fs=fs, remote=str(path.parent)[1:]),
      )
      if req.status_code != 200:
        raise FileNotFoundError()
      ret = req.json()
      logger.info(f"{ret}")
      try:
        item = next(iter(item for item in ret['list'] if item['Name'] == path.name))
      except StopIteration as e:
        raise FileNotFoundError() from e
      logger.info(f"{item}")
      if item['IsDir']:
        return {
          'type': 'directory',
          'size': 0,
          # 'mtime': item['ModTime'],
        }
      else:
        return {
          'type': 'file',
          'size': item['Size'],
          # 'mtime': item['ModTime'],
        }

  def cat(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    req = requests.post(
      f"{self._url}/core/command",
      auth=self._auth,
      json=dict(
        command='cat',
        arg=json.dumps([fs + str(path)[1:]]),
        returnType='STREAM_ONLY_STDOUT',
      ),
      stream=True,
    )
    if req.status_code != 200:
      logger.debug(req.text)
      raise FileNotFoundError()
    yield from rstrip_iter(req.iter_content(self.CHUNK_SIZE), b'{}\n')

  def put(self, path, data, *, size_hint=None):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    requests.post(
      f"{self._url}/operations/uploadfile",
      auth=self._auth,
      params=dict(fs=fs, remote=str(path.parent)[1:]),
      files={'file0': (path.name, ReadableIterator(data), 'application/octet-stream')},
    )

  def unlink(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    req = requests.post(
      f"{self._url}/operations/deletefile",
      auth=self._auth,
      params=dict(fs=fs, remote=str(path)[1:])
    )
    if req.status_code != 200:
      raise FileNotFoundError()

  def mkdir(self, path):
    fs, remote = rclone_uri_from_path(path)
    if not fs:
      raise FileExistsError()
    if not remote:
      try:
        self.info(path)
      except FileNotFoundError:
        raise PermissionError()
      else:
        raise FileExistsError()
    req = requests.post(
      f"{self._url}/operations/mkdir",
      auth=self._auth,
      params=dict(fs=fs, remote=str(remote)[1:]),
    )
    ret = req.json()

  def rmdir(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    req = requests.post(
      f"{self._url}/operations/rmdir",
      auth=self._auth,
      params=dict(fs=fs, remote=str(path)[1:]),
    )
    ret = req.json()

  def copy(self, src, dst):
    src_fs, src_path = rclone_uri_from_path(src)
    dst_fs, dst_path = rclone_uri_from_path(dst)
    if not src_fs or not dst_fs: raise PermissionError()
    if not src_path or not dst_path: raise PermissionError()
    req = requests.post(
      f"{self._url}/operations/copyfile",
      auth=self._auth,
      params=dict(
        srcFs=src_fs,
        srcRemote=str(src_path)[1:],
        dstFs=dst_fs,
        dstRemote=str(dst_path)[1:],
      )
    )
    ret = req.json()

  def rename(self, src, dst):
    src_fs, src_path = rclone_uri_from_path(src)
    dst_fs, dst_path = rclone_uri_from_path(dst)
    if not src_fs or not dst_fs: raise PermissionError()
    if not src_path or not dst_path: raise PermissionError()
    req = requests.post(
      f"{self._url}/operations/movefile",
      auth=self._auth,
      params=dict(
        srcFs=src_fs,
        srcRemote=str(src_path)[1:],
        dstFs=dst_fs,
        dstRemote=str(dst_path)[1:],
      ),
    )
    ret = req.json()
