''' The UFS operates another in an independent process
'''

import typing as t
import multiprocessing as mp
from ufs.spec import UFS

def ufs_proc(send: mp.Queue, recv: mp.Queue, ufs_spec):
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
    self._ensure_proc()
    self._send.put([op, args, kwargs])
    ret, err = self._recv.get(timeout=1)
    if err is not None: raise err
    else: return ret

  def ls(self, path: str) -> list[str]:
    return self._forward('ls', path)

  def info(self, path: str) -> list[str]:
    return self._forward('info', path)

  def open(self, path: str, mode: t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']) -> int:
    return self._forward('open', path, mode)

  def seek(self, fd: int, pos: int, whence: t.Literal[0, 1, 2] = 0):
    return self._forward('seek', fd, pos, whence)

  def read(self, fd: int, amnt: int) -> bytes:
    return self._forward('read', fd, amnt)

  def write(self, fd: int, data: bytes) -> int:
    return self._forward('write', fd, data)

  def truncate(self, fd: int, length: int):
    return self._forward('truncate', fd, length)

  def close(self, fd: int):
    return self._forward('close', fd)

  def unlink(self, path: str):
    return self._forward('unlink', path)

  # optional
  def mkdir(self, path: str):
    return self._forward('mkdir', path)
  def rmdir(self, path: str):
    return self._forward('rmdir', path)
  def flush(self, fd: int):
    return self._forward('flush', fd)

  # fallback
  def copy(self, src: str, dst: str):
    return self._forward('copy', src, dst)

  def rename(self, src: str, dst: str):
    return self._forward('rename', src, dst)

  def start(self):
    if not hasattr(self, '_proc'):
      self._send, self._recv = mp.Queue(), mp.Queue()
      self._proc = mp.Process(target=ufs_proc, args=(self._recv, self._send, self._ufs.to_dict()))
      self._proc.start()
      self._proc('start')

  def stop(self):
    if hasattr(self, '_proc'):
      self._forward('stop')
      self._send.put(None)
      self._proc.join()

  def __repr__(self) -> str:
    return f"Process({repr(self._ufs)})"
