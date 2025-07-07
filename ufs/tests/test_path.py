import pytest
from ufs.access.pathlib import UPath, AsyncUPath
from ufs.impl.sync import Sync
from ufs.impl.asyn import Async

from ufs.tests.fixtures import ufs
def test_path(ufs):
  ''' Actually test that filesystem ops work as expected
  '''
  path = UPath(Sync(ufs)) / 'pathlib'
  path.mkdir(parents=True, exist_ok=True)
  assert path.is_dir()
  with (path/'A').open('w') as fw:
    assert (path/'A').is_file()
    fw.write('Hello World!')
  with (path/'A').open('r+') as fh:
    assert fh.read() == 'Hello World!'
    fh.seek(0)
    fh.write('h')
    fh.seek(6)
    fh.write('w')
    fh.seek(0)
    assert fh.read() == 'hello world!'
  (path/'A').rename(path/'B')
  with (path/'B').open('a') as fa:
    fa.write('\n!')
  with pytest.raises(FileNotFoundError): (path/'A').read_text()
  with (path/'B').open('r') as fr:
    fr = iter(fr)
    assert next(fr) == 'hello world!\n'
    assert next(fr) == '!'
    with pytest.raises(StopIteration): next(fr)
  assert [p.name for p in path.iterdir()] == ['B']
  (path/'B').unlink()
  assert not (path/'B').exists()

@pytest.mark.asyncio
async def test_async_path(ufs):
  ''' Actually test that filesystem ops work as expected
  '''
  path = AsyncUPath(Async(ufs)) / 'pathlib'
  await path.mkdir(parents=True, exist_ok=True)
  assert await path.is_dir()
  async with await (path/'A').open('w') as fw:
    assert await (path/'A').is_file()
    await fw.write('Hello World!')
  assert await (path/'A').is_file()
  async with await (path/'A').open('r+') as fh:
    assert await fh.read() == 'Hello World!'
    await fh.seek(0)
    await fh.write('h')
    await fh.seek(6)
    await fh.write('w')
    await fh.seek(0)
    assert await fh.read() == 'hello world!'
  await (path/'A').rename(path/'B')
  async with await (path/'B').open('a') as fa:
    await fa.write('\n!')
  with pytest.raises(FileNotFoundError): await (path/'A').read_text()
  async with await (path/'B').open('r') as fr:
    fr = aiter(fr)
    assert await fr.__anext__() == 'hello world!\n'
    assert await fr.__anext__() == '!'
    with pytest.raises(StopAsyncIteration): await fr.__anext__()
  assert [p.name async for p in path.iterdir()] == ['B']
  await (path/'B').unlink()
  assert not await (path/'B').exists()
