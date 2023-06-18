''' Access any `rclone`-supported protocol
'''

import sys
import uuid
import queue
import socket
import logging
import requests
import itertools
import threading as t
from ufs.spec import UFS
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

class QueueBufferedReader(queue.Queue):
  def __init__(self, maxsize: int = 0) -> None:
    super().__init__(maxsize)
    self.buffer = b''
  def read(self, amnt = -1):
    while amnt == -1 or len(self.buffer) < amnt:
      buf = self.get()
      if buf is None: break
      self.buffer += buf
    if amnt == -1:
      ret = self.buffer
    else:
      ret = self.buffer[:amnt]
    self.buffer = self.buffer[len(ret):]
    return ret
  def write(self, data: bytes):
    self.put(data)

class RClone(UFS):
  def __init__(self, env: dict = {}):
    super().__init__()
    self._env = env
    self._cfd = iter(itertools.count(start=5))
    self._fds = {}

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
      req = requests.post(f"http://{self._host}:{self._port}/config/listremotes", auth=(self._user, self._pass), json=dict())
      if req.status_code != 200:
        raise FileNotFoundError()
      ret = req.json()
      return ret['remotes']
    else:
      req = requests.post(f"http://{self._host}:{self._port}/operations/list", auth=(self._user, self._pass), params=dict(fs=fs, remote=str(path)[1:]), json=dict())
      if req.status_code != 200:
        raise FileNotFoundError()
      ret = req.json()
      return [item['Name'] for item in ret['list']]

  def info(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs:
      return { 'type': 'directory', 'size': 0 }
    if not path:
      req = requests.post(f"http://{self._host}:{self._port}/operations/fsinfo", auth=(self._user, self._pass), params=dict(fs=fs), json=dict())
      if req.status_code != 200:
        raise FileNotFoundError
      return { 'type': 'directory', 'size': 0 }
    else:
      req = requests.post(f"http://{self._host}:{self._port}/operations/list", auth=(self._user, self._pass), params=dict(fs=fs, remote=str(path.parent)[1:]), json=dict())
      if req.status_code != 200:
        raise FileNotFoundError()
      ret = req.json()
      try:
        item = next(iter(item for item in ret['list'] if item['Name'] == path.name))
      except StopIteration:
        raise FileNotFoundError()
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

  def open(self, path, mode, *, size_hint = None):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if '+' in mode:
      raise NotImplementedError(mode)
    if 'r' in mode:
      req = requests.get(f"http://{self._host}:{self._port}/[{fs}]{path}", auth=(self._user, self._pass))
      if req.status_code != 200:
        raise FileNotFoundError()
      fd = next(self._cfd)
      self._fds[fd] = dict(mode='r', fs=fs, path=path, req=req)
    elif 'w' in mode:
      fd = next(self._cfd)
      pipe = QueueBufferedReader()
      thread = t.Thread(
        target=requests.post,
        args=(f"http://{self._host}:{self._port}/operations/uploadfile",),
        kwargs=dict(
          auth=(self._user, self._pass),
          params=dict(fs=fs, remote=str(path)[1:]),
          files={
            'file0': (path.name, pipe, 'application/octet-stream'),
          }
        ),
      )
      thread.start()
      self._fds[fd] = dict(mode='w', fs=fs, path=path, thread=thread, pipe=pipe)
    else:
      raise NotImplementedError(mode)
    return fd
  def seek(self, fd, pos, whence = 0):
    raise NotImplementedError()
  def read(self, fd, amnt):
    if self._fds[fd]['mode'] != 'r': raise NotImplementedError()
    return self._fds[fd]['req'].read(amnt)
  def write(self, fd, data):
    if self._fds[fd]['mode'] != 'w': raise NotImplementedError()
    self._fds[fd]['pipe'].write(data)
    return len(data)
  def truncate(self, fd, length):
    raise NotImplementedError()
  def flush(self, fd):
    pass
  def close(self, fd):
    descriptor = self._fds.pop(fd)
    if descriptor['mode'] == 'w':
      descriptor['pipe'].write(None)
      descriptor['thread'].join()

  def unlink(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    req = requests.post(f"http://{self._host}:{self._port}/operations/deletefile", auth=(self._user, self._pass), json=dict(
      fs=fs,
      remote=str(path)[1:],
    ))
    if req.status_code != 200:
      raise FileNotFoundError()

  def mkdir(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    req = requests.post(f"http://{self._host}:{self._port}/operations/mkdir", auth=(self._user, self._pass), json=dict(
      fs=fs,
      remote=str(path)[1:],
    ))
    ret = req.json()

  def rmdir(self, path):
    fs, path = rclone_uri_from_path(path)
    if not fs: raise PermissionError()
    if not path: raise PermissionError()
    req = requests.post(f"http://{self._host}:{self._port}/operations/rmdir", auth=(self._user, self._pass), json=dict(
      fs=fs,
      remote=str(path)[1:],
    ))
    ret = req.json()

  def copy(self, src, dst):
    src_fs, src_path = rclone_uri_from_path(src)
    dst_fs, dst_path = rclone_uri_from_path(dst)
    if not src_fs or not dst_fs: raise PermissionError()
    if not src_path or not dst_path: raise PermissionError()
    req = requests.post(f"http://{self._host}:{self._port}/operations/copyfile", auth=(self._user, self._pass), json=dict(
      srcFs=src_fs,
      srcRemote=str(src_path)[1:],
      dstFs=dst_fs,
      dstRemote=str(dst_path)[1:],
    ))
    ret = req.json()

  def rename(self, src, dst):
    src_fs, src_path = rclone_uri_from_path(src)
    dst_fs, dst_path = rclone_uri_from_path(dst)
    if not src_fs or not dst_fs: raise PermissionError()
    if not src_path or not dst_path: raise PermissionError()
    req = requests.post(f"http://{self._host}:{self._port}/operations/movefile", auth=(self._user, self._pass), json=dict(
      srcFs=src_fs,
      srcRemote=str(src_path)[1:],
      dstFs=dst_fs,
      dstRemote=str(dst_path)[1:],
    ))
    ret = req.json()
