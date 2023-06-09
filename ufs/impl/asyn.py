import asyncio
from ufs.spec import UFS, AsyncUFS

def ufs_thread(loop: asyncio.AbstractEventLoop, send: asyncio.Queue, recv: asyncio.Queue, ufs_spec):
  ufs = UFS.from_dict(**ufs_spec)
  while True:
    msg = asyncio.run_coroutine_threadsafe(recv.get(), loop).result()
    if not msg: break
    op, args, kwargs = msg
    try:
      func = getattr(ufs, op)
      res = func(*args, **kwargs)
    except Exception as err:
      asyncio.run_coroutine_threadsafe(send.put([None, err]), loop).result()
    else:
      asyncio.run_coroutine_threadsafe(send.put([res, None]), loop).result()

async def run_ufs(send: asyncio.Queue, recv: asyncio.Queue, ufs_spec):
  loop = asyncio.get_event_loop()
  await loop.run_in_executor(None, ufs_thread, loop, send, recv, ufs_spec)

class Async(AsyncUFS):
  def __init__(self, ufs: UFS):
    super().__init__()
    self._ufs = ufs

  @staticmethod
  def from_dict(*, ufs):
    return Async(
      ufs=UFS.from_dict(**ufs),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
    )
  
  async def _forward(self, op, *args, **kwargs):
    await self.start()
    await self._send.put([op, args, kwargs])
    ret, err = await self._recv.get()
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
      self._send, self._recv = asyncio.Queue(), asyncio.Queue()
      self._task = asyncio.create_task(run_ufs(self._recv, self._send, self._ufs.to_dict()))
      await self._forward('start')

  async def stop(self):
    if hasattr(self, '_loop'):
      await self._forward('stop')
      await self._send.put(None)
      await self._task