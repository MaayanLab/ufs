''' Access any `rclone`-supported protocol
'''

import sys
import uuid
import json
import socket
import logging
import requests
from ufs.spec import UFS, DescriptorFromAtomicMixin, ReadableIterator
from ufs.utils.pathlib import SafePurePosixPath
from subprocess import Popen

logger = logging.getLogger(__name__)

def rclone_uri_from_path(path):
  if path == SafePurePosixPath('/'):
    return None, None
  _, fs, *parts = path.parts
  if not parts:
    return fs, None
  return fs+':', SafePurePosixPath('/'.join(parts))

def rstrip_iter(it, rstrip):
  last = None
  for el in it:
    if last is not None:
      yield last
    last = el
  yield last.rstrip(rstrip)

class RClone(DescriptorFromAtomicMixin, UFS):
  def __init__(self, env: dict = {}):
    super().__init__()
    self._env = env

  @staticmethod
  def from_dict(*, env):
    return RClone(
      env=env,
    )

  def to_dict(self):
    return dict(super().to_dict(),
      env=self._env,
    )

  def start(self):
    if not hasattr(self, '_proc'):
      with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        self._host, self._port = s.getsockname()
      self._user, self._pass = str(uuid.uuid4()), str(uuid.uuid4())
      self._proc = Popen([
        'rclone', 'rcd',
        '--rc-addr', f"{self._host}:{self._port}",
        '--rc-user', self._user,
        '--rc-pass', self._pass,
      ], env=self._env, stderr=sys.stderr, stdout=sys.stdout)
      # TODO: wait until it's ready
      import time; time.sleep(1)

  def stop(self):
    if hasattr(self, '_proc'):
      self._proc.terminate()
      self._proc.wait()
      del self._proc

  def ls(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs:
      req = requests.post(
        f"http://{self._host}:{self._port}/config/listremotes",
        auth=(self._user, self._pass),
      )
      if req.status_code != 200:
        raise FileNotFoundError()
      ret = req.json()
      return ret['remotes']
    else:
      req = requests.post(
        f"http://{self._host}:{self._port}/operations/list",
        auth=(self._user, self._pass),
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
        f"http://{self._host}:{self._port}/operations/fsinfo",
        auth=(self._user, self._pass),
        params=dict(fs=fs),
      )
      if req.status_code != 200:
        raise FileNotFoundError
      return { 'type': 'directory', 'size': 0 }
    else:
      req = requests.post(
        f"http://{self._host}:{self._port}/operations/list",
        auth=(self._user, self._pass),
        params=dict(fs=fs, remote=str(path.parent)[1:]),
      )
      if req.status_code != 200:
        raise FileNotFoundError()
      ret = req.json()
      logger.info(f"{ret=}")
      try:
        item = next(iter(item for item in ret['list'] if item['Name'] == path.name))
      except StopIteration:
        raise FileNotFoundError()
      logger.info(f"{item=}")
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
      f"http://{self._host}:{self._port}/core/command",
      auth=(self._user, self._pass),
      json=dict(
        command='cat',
        arg=json.dumps([fs + str(path)[1:]]),
        returnType='STREAM_ONLY_STDOUT',
      ),
      stream=True,
    )
    if req.status_code != 200:
      raise FileNotFoundError()
    yield from rstrip_iter(req.iter_content(self.CHUNK_SIZE), b'{}\n')

  def put(self, path, data, *, size_hint=None):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    requests.post(
      f"http://{self._host}:{self._port}/operations/uploadfile",
      auth=(self._user, self._pass),
      params=dict(fs=fs, remote=str(path.parent)[1:]),
      files={'file0': (path.name, ReadableIterator(data), 'application/octet-stream')},
    )

  def unlink(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    req = requests.post(
      f"http://{self._host}:{self._port}/operations/deletefile",
      auth=(self._user, self._pass),
      params=dict(fs=fs, remote=str(path)[1:])
    )
    if req.status_code != 200:
      raise FileNotFoundError()

  def mkdir(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    req = requests.post(
      f"http://{self._host}:{self._port}/operations/mkdir",
      auth=(self._user, self._pass),
      params=dict(fs=fs, remote=str(path)[1:]),
    )
    ret = req.json()

  def rmdir(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    req = requests.post(
      f"http://{self._host}:{self._port}/operations/rmdir",
      auth=(self._user, self._pass),
      params=dict(fs=fs, remote=str(path)[1:]),
    )
    ret = req.json()

  def copy(self, src, dst):
    src_fs, src_path = rclone_uri_from_path(src)
    dst_fs, dst_path = rclone_uri_from_path(dst)
    if not src_fs or not dst_fs: raise PermissionError()
    if not src_path or not dst_path: raise PermissionError()
    req = requests.post(
      f"http://{self._host}:{self._port}/operations/copyfile",
      auth=(self._user, self._pass),
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
      f"http://{self._host}:{self._port}/operations/movefile",
      auth=(self._user, self._pass),
      params=dict(
        srcFs=src_fs,
        srcRemote=str(src_path)[1:],
        dstFs=dst_fs,
        dstRemote=str(dst_path)[1:],
      ),
    )
    ret = req.json()
