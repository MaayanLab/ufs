''' Turn any AsyncUFS into a regular UFS by running it in a dedicated event loop thread.
'''
import asyncio
import itertools
import threading as t
from ufs.spec import UFS, AsyncUFS

class AsyncExecutor:
  ''' Similar to ThreadPoolExecutor but submit & manage asyncio tasks
  '''
  async def __aenter__(self):
    self.task_id = iter(itertools.count())
    self.tasks = {}
    self.closing = False
    return self
  async def _invoke(self, task_id, fn, *args, **kwargs):
    await fn(*args, **kwargs)
    del self.tasks[task_id]
  def submit(self, fn, *args, **kwargs):
    if self.closing: raise asyncio.CancelledError()
    task_id = next(self.task_id)
    self.tasks[task_id] = asyncio.create_task(self._invoke(task_id, fn, *args, **kwargs))
  async def __aexit__(self, *args):
    self.closing = True
    await asyncio.gather(*self.tasks.values())
    del self.tasks

async def _run_and_return(ufs, send, task_id, op, args, kwargs):
  try:
    func = getattr(ufs, op)
    res = await func(*args, **kwargs)
  except Exception as err:
    await send.put([task_id, None, err])
  else:
    await send.put([task_id, res, None])

async def async_ufs_proc(send: asyncio.PriorityQueue, recv: asyncio.PriorityQueue, ufs_spec):
  ufs = UFS.from_dict(**ufs_spec)
  async with AsyncExecutor() as exec:
    while True:
      msg = await recv.get()
      i, op, args, kwargs = msg
      if op == None:
        recv.task_done()
        break
      exec.submit(_run_and_return, ufs, send, *msg)
      recv.task_done()

def event_loop_thread(loop, send: asyncio.PriorityQueue, recv: asyncio.PriorityQueue, ufs_spec):
  loop.run_until_complete(async_ufs_proc(send, recv, ufs_spec))

class Sync(UFS):
  def __init__(self, ufs: AsyncUFS):
    super().__init__()
    self._ufs = ufs
    self._taskid = iter(itertools.count())

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
    i, i_ = next(self._taskid), None
    asyncio.run_coroutine_threadsafe(self._send.put([i, op, args, kwargs]), self._loop).result()
    while True:
      i_, ret, err = asyncio.run_coroutine_threadsafe(self._recv.get(), self._loop).result()
      self._recv.task_done()
      if i == i_:
        break
      # a different result came before ours, add it back to the queue and try again
      asyncio.run_coroutine_threadsafe(self._recv.put([i_, ret, err]), self._loop).result()
    if err is not None: raise err
    else: return ret

  def ls(self, path):
    return self._forward('ls', path)

  def info(self, path):
    return self._forward('info', path)

  def open(self, path, mode, *, size_hint = None):
    return self._forward('open', path, mode, size_hint=size_hint)

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
      self._send, self._recv = asyncio.PriorityQueue(), asyncio.PriorityQueue()
      self._loop_thread = t.Thread(
        target=event_loop_thread,
        args=(self._loop, self._recv, self._send, self._ufs.to_dict()),
      )
      self._loop_thread.start()
      self._forward('start')

  def stop(self):
    if hasattr(self, '_loop'):
      self._forward('stop')
      asyncio.run_coroutine_threadsafe(self._send.put([next(iter(self._taskid)), None, None, None]), self._loop).result()
      self._loop_thread.join()
      self._loop.close()
      del self._loop
