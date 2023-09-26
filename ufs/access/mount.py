''' Use fuse if libfuse is available, otherwise fallback to ffuse for mounting.
'''

import asyncio
import contextlib
from ufs.spec import UFS

@contextlib.contextmanager
def mount(ufs: UFS, mount_dir: str = None, readonly: bool = False):
  try:
    from ufs.access.fuse import fuse_mount as _mount
  except ImportError:
    import logging; logging.getLogger(__name__).warning('Install fusepy for proper fuse mounting, falling back to ffuse')
    from ufs.access.ffuse import ffuse_mount as _mount
  except OSError:
    import logging; logging.getLogger(__name__).warning('Install libfuse for proper fuse mounting, falling back to ffuse')
    from ufs.access.ffuse import ffuse_mount as _mount
  with _mount(ufs, mount_dir, readonly=readonly) as p:
    yield p

def _async_mount_thread(loop: asyncio.AbstractEventLoop, send: asyncio.Queue, completed: asyncio.Event, ufs: UFS, mount_dir: str = None, readonly: bool = False):
  try:
    with mount(ufs, mount_dir, readonly) as mount_dir:
      asyncio.run_coroutine_threadsafe(send.put(mount_dir), loop).result()
      asyncio.run_coroutine_threadsafe(completed.wait(), loop).result()
  except Exception as e:
    asyncio.run_coroutine_threadsafe(send.put(e), loop).result()
  
async def async_mount_task(ufs: UFS, mount_dir: str = None, readonly: bool = False, *, task_status = None):
  import anyio
  if task_status is None:
    task_status = anyio.TASK_STATUS_IGNORED
  loop = asyncio.get_event_loop()
  completed = asyncio.Event()
  recv = asyncio.Queue()
  task = loop.run_in_executor(None, _async_mount_thread, loop, recv, completed, ufs, mount_dir, readonly)
  mount_dir = await recv.get()
  recv.task_done()
  task_status.started(mount_dir)
  try:
    err = await recv.get()
  except asyncio.CancelledError:
    completed.set()
    await task
    raise
  else:
    if err is not None:
      raise err

@contextlib.asynccontextmanager
async def async_mount(ufs: UFS, mount_dir: str = None, readonly: bool = False):
  import anyio
  import pathlib, tempfile
  mount_dir_resolved = pathlib.Path(tempfile.mkdtemp() if mount_dir is None else mount_dir)
  async with anyio.create_task_group() as tg:
    await tg.start(async_mount_task, ufs, mount_dir_resolved, readonly)
    try:
      yield mount_dir_resolved
    finally:
      tg.cancel_scope.cancel()
