import asyncio
import threading as t
from ufs.spec import UFS, AsyncUFS

async def async_ufs_proc(send: asyncio.Queue, recv: asyncio.Queue, ufs_spec):
  ufs = UFS.from_dict(**ufs_spec)
  while True:
    msg = await recv.get()
    if not msg: break
    op, args, kwargs = msg
    try:
      func = getattr(ufs, op)
      res = await func(*args, **kwargs)
    except Exception as err:
      await send.put([None, err])
    else:
      await send.put([res, None])

def event_loop_thread(loop, send: asyncio.Queue, recv: asyncio.Queue, ufs_spec):
  loop.run_until_complete(async_ufs_proc(send, recv, ufs_spec))

class Sync(UFS):
  def __init__(self, ufs: AsyncUFS):
    super().__init__()
    self._ufs = ufs

  @staticmethod
  def from_dict(*, ufs):
    return Sync(
      ufs=UFS.from_dict(**ufs),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
    )
  
  def _forward(self, op, *args, **kwargs):
    self.start()
    asyncio.run_coroutine_threadsafe(self._send.put([op, args, kwargs]), self._loop).result()
    ret, err = asyncio.run_coroutine_threadsafe(self._recv.get(), self._loop).result()
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
    if not hasattr(self, '_loop'):
      self._loop = asyncio.new_event_loop()
      self._send, self._recv = asyncio.Queue(), asyncio.Queue()
      self._loop_thread = t.Thread(target=event_loop_thread, args=(self._loop, self._recv, self._send, self._ufs.to_dict()))
      self._loop_thread.start()
      self._forward('start')

  def stop(self):
    if hasattr(self, '_loop'):
      self._forward('stop')
      self._send.put(None)
      self._loop_thread.join()
      self._loop.stop()
