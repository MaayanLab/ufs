''' The UFS operates another in an independent process
'''

import typing as t
import multiprocessing as mp
from ufs.spec import UFS

mp_spawn = mp.get_context('spawn')

def ufs_proc(send: mp_spawn.Queue, recv: mp_spawn.Queue, ufs_spec):
  ufs = UFS.from_dict(**ufs_spec)
  while True:
    msg = recv.get()
    if not msg: break
    op, args, kwargs = msg
    try:
      send.put([getattr(ufs, op)(*args, **kwargs), None])
    except Exception as err:
      send.put([None, err])

class Process(UFS):
  def __init__(self, ufs: UFS):
    super().__init__()
    self._ufs = ufs

  @staticmethod
  def from_dict(*, ufs):
    return Process(
      ufs=UFS.from_dict(**ufs),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
    )
  
  def _forward(self, op, *args, **kwargs):
    self.start()
    self._send.put([op, args, kwargs])
    ret, err = self._recv.get()
    if err is not None: raise err
    else: return ret

  def ls(self, path):
    return self._forward('ls', path)

  def info(self, path):
    return self._forward('info', path)

  def open(self, path, mode):
    return self._forward('open', path, mode)

  def seek(self, fd, pos, whence = 0):
    return self._forward('seek', fd, pos, whence)

  def read(self, fd, amnt):
    return self._forward('read', fd, amnt)

  def write(self, fd, data: bytes):
    return self._forward('write', fd, data)

  def truncate(self, fd, length):
    return self._forward('truncate', fd, length)

  def close(self, fd):
    return self._forward('close', fd)

  def unlink(self, path):
    return self._forward('unlink', path)

  # optional
  def mkdir(self, path):
    return self._forward('mkdir', path)
  def rmdir(self, path):
    return self._forward('rmdir', path)
  def flush(self, fd):
    return self._forward('flush', fd)

  # fallback
  def copy(self, src, dst):
    return self._forward('copy', src, dst)

  def rename(self, src, dst):
    return self._forward('rename', src, dst)

  def start(self):
    if not hasattr(self, '_proc'):
      self._send, self._recv = mp_spawn.Queue(), mp_spawn.Queue()
      self._proc = mp_spawn.Process(target=ufs_proc, args=(self._recv, self._send, self._ufs.to_dict()))
      self._proc.start()
      self._forward('start')

  def stop(self):
    if hasattr(self, '_proc'):
      self._forward('stop')
      self._send.put(None)
      self._proc.join()
