import os
import json
import asyncio
import msgpack
import contextlib
from ufs.spec import UFS, AsyncUFS
from ufs.impl.asyn import Async

def handle_connection_factory(ufs: AsyncUFS):
  async def handle_connection(reader, writer):
    async for msg in reader:
      i, op, args, kwargs = msgpack.unpackb(msg, raw=False)
      if op is None: break
      try:
        func = getattr(ufs, op)
        res = await func(*args, kwargs)
      except Exception as err:
        await writer.write(msgpack.packb([i, None, err], use_bin_type=True))
      else:
        await writer.write(msgpack.packb([i, res, None], use_bin_type=True))
    writer.close()
    await writer.wait_closed()
  return handle_connection

async def async_ufs_via_socket(ufs: UFS, host, port):
  async with Async(ufs) as ufs:
    server = await asyncio.start_server(handle_connection_factory(ufs), host, port)
    print(', '.join(':'.join(map(str,sock.getsockname())) for sock in server.sockets))
    async with server:
      await server.serve_forever()

def ufs_via_socket(ufs: dict, host: str, port: int):
  asyncio.get_event_loop().run_until_complete(async_ufs_via_socket(UFS.from_dict(**ufs), host, port))

@contextlib.contextmanager
def serve_ufs_via_socket(ufs: UFS, host: str = '', port: int = 0):
  import multiprocessing as mp
  from ufs.utils.process import active_process
  from ufs.utils.polling import wait_for
  from ufs.utils.socket import nc_z, autosocket
  mp_spawn = mp.get_context('spawn')
  host, port = autosocket(host, port)
  with active_process(mp_spawn.Process(
    target=ufs_via_socket,
    kwargs=dict(
      ufs=ufs.to_dict(),
      host=host,
      port=port,
    ),
  )):
    wait_for(lambda: nc_z(host, port))
    yield f"{host}:{port}"

if __name__ == '__main__':
  ufs = UFS.from_dict(**json.loads(os.environ.pop('UFS_SPEC')))
  host, _, port = (os.environ.get('UFS_BIND') or '127.0.0.1:0').partition(':')
  ufs_via_socket(ufs.to_dict(), host, int(port))
