''' Use fuse if libfuse is available, otherwise fallback to ffuse for mounting.
'''

import pathlib
import asyncio
import logging
import contextlib
import typing as t
from ufs.spec import UFS

logger = logging.getLogger(__name__)

@contextlib.contextmanager
def mount(ufs: UFS, mount_dir: t.Optional[str] = None, readonly: bool = False, fuse: t.Optional[bool] = None):
  if fuse is None:
    try:
      from ufs.access.fuse import fuse_mount as _mount
    except ImportError:
      logger.warning('Install fusepy for proper fuse mounting, falling back to ffuse')
      from ufs.access.ffuse import ffuse_mount as _mount
    except OSError:
      logger.warning('Install libfuse for proper fuse mounting, falling back to ffuse')
      from ufs.access.ffuse import ffuse_mount as _mount
  elif fuse:
    from ufs.access.fuse import fuse_mount as _mount
  else:
    from ufs.access.ffuse import ffuse_mount as _mount
  with _mount(ufs, mount_dir, readonly=readonly) as mount_path:
    yield mount_path

def _async_mount_thread(loop: asyncio.AbstractEventLoop, send: asyncio.Queue, completed: asyncio.Event, ufs: UFS, mount_dir: str = None, readonly: bool = False, fuse: bool = None):
  with mount(ufs, mount_dir, readonly, fuse) as mount_path:
    asyncio.run_coroutine_threadsafe(send.put(mount_path), loop).result()
    asyncio.run_coroutine_threadsafe(completed.wait(), loop).result()

async def to_thread(loop, func, *args):
  return await loop.run_in_executor(None, func, *args)

@contextlib.asynccontextmanager
async def async_mount(ufs: UFS, mount_dir: t.Optional[str] = None, readonly: bool = False, fuse: t.Optional[bool] = None) -> t.AsyncGenerator[pathlib.Path, t.Any]:
  loop = asyncio.get_event_loop()
  completed = asyncio.Event()
  recv = asyncio.Queue()
  mount_task = asyncio.create_task(to_thread(loop, _async_mount_thread, loop, recv, completed, ufs, mount_dir, readonly, fuse))
  mount_task.add_done_callback(asyncio.Task.result)
  mount_path = await recv.get()
  recv.task_done()
  try:
    yield mount_path
  finally:
    completed.set()
    await mount_task
