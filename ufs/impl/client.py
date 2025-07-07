import json
import asyncio
import itertools
import msgpack
from ufs.spec import AsyncUFS

class Client(AsyncUFS):
  def __init__(self, uri) -> None:
    super().__init__()
    self._uri = uri
    self._taskid = iter(itertools.count())
  
  @staticmethod
  def from_dict(*, uri):
    return Client(
      uri=uri,
    )

  def to_dict(self):
    return dict(super().to_dict(),
      uri=self._uri,
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

  async def _sync_reads(self, reader):
    async for msg in reader:
      await self._recv.put(msgpack.unpackb(msg, raw=False))
  
  async def _sync_writes(self, writer):
    while True:
      i, op, args, kwargs = await self._send.get()
      if op is None:
        self._send.task_done()
        break
      await writer.write(msgpack.packb([i, op, args, kwargs], use_bin_type=True))
      self._send.task_done()
    writer.close()
    await writer.wait_closed()

  async def _client(self):
    host, port = self._uri.partition(':')
    port = int(port)
    reader, writer = await asyncio.open_connection(host, port)
    await asyncio.gather(self._sync_reads(reader), self._sync_writes(writer))

  async def start(self):
    if not hasattr(self, '_task'):
      self._send, self._recv = asyncio.PriorityQueue(), asyncio.PriorityQueue()
      self._task = asyncio.create_task(self._client())
      await self._forward('start')

  async def stop(self):
    if hasattr(self, '_task'):
      await self._forward('stop')
      await self._send.put([next(self._taskid), None, None, None])
      await self._task
      del self._task
