''' Treat any sync UFS as an AsyncUFS, it runs in a dedicated thread
'''
import asyncio
import itertools
import typing as t
from ufs.spec import UFS, SyncUFS, AsyncUFS

def ufs_thread(loop: asyncio.AbstractEventLoop, send: asyncio.PriorityQueue, recv: asyncio.PriorityQueue, ufs_spec):
  ufs = UFS.from_dict(**ufs_spec)
  while True:
    msg = asyncio.run_coroutine_threadsafe(recv.get(), loop).result()
    i, op, args, kwargs = msg
    if op is None:
      recv.task_done()
      break
    try:
      func = getattr(ufs, op)
      res = func(*args, **kwargs)
    except Exception as err:
      asyncio.run_coroutine_threadsafe(send.put([i, None, err]), loop).result()
    else:
      asyncio.run_coroutine_threadsafe(send.put([i, res, None]), loop).result()
    recv.task_done()

class Async(AsyncUFS):
  def __new__(cls, ufs: t.Union[UFS, AsyncUFS]):
    # Prevent unnecessary compositional recursion
    from ufs.impl.sync import Sync
    if isinstance(ufs, AsyncUFS): return ufs
    elif isinstance(ufs, Sync): return ufs._ufs
    else: return super().__new__(cls)

  def __init__(self, ufs: SyncUFS):
    super().__init__()
    self._ufs = ufs
    self._taskid = iter(itertools.count())

  def scope(self):
    return self._ufs.scope()

  @staticmethod
  def from_dict(*, ufs):
    return Async(
      ufs=AsyncUFS.from_dict(**ufs),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
    )
  
  async def _forward(self, op, *args, **kwargs):
    await self.start()
    i, i_ = next(self._taskid), None
    await self._send.put([i, op, args, kwargs])
    while True:
      i_, ret, err = await self._recv.get()
      self._recv.task_done()
      if i == i_:
        break
      # a different result came before ours, add it back to the queue and try again
      await self._recv.put([i_, ret, err])
    if err is not None: raise err
    else: return ret

  async def ls(self, path):
    return await self._forward('ls', path)

  async def info(self, path):
    return await self._forward('info', path)

  async def open(self, path, mode, *, size_hint = None):
    return await self._forward('open', path, mode, size_hint=size_hint)

  async def seek(self, fd, pos, whence = 0):
    return await self._forward('seek', fd, pos, whence)

  async def read(self, fd, amnt):
    return await self._forward('read', fd, amnt)

  async def write(self, fd, data: bytes):
    return await self._forward('write', fd, data)

  async def truncate(self, fd, length):
    return await self._forward('truncate', fd, length)

  async def close(self, fd):
    return await self._forward('close', fd)

  async def unlink(self, path):
    return await self._forward('unlink', path)

  # optional
  async def mkdir(self, path):
    return await self._forward('mkdir', path)
  async def rmdir(self, path):
    return await self._forward('rmdir', path)
  async def flush(self, fd):
    return await self._forward('flush', fd)

  # fallback
  async def copy(self, src, dst):
    return await self._forward('copy', src, dst)

  async def rename(self, src, dst):
    return await self._forward('rename', src, dst)

  async def start(self):
    if not hasattr(self, '_task'):
      self._send, self._recv = asyncio.PriorityQueue(), asyncio.PriorityQueue()
      loop = asyncio.get_event_loop()
      self._task = loop.run_in_executor(None, ufs_thread, loop, self._recv, self._send, self._ufs.to_dict())
      await self._forward('start')

  async def stop(self):
    if hasattr(self, '_task'):
      await self._forward('stop')
      await self._send.put([next(self._taskid), None, None, None])
      await self._task
      del self._task
