import pytest
import tempfile
import pathlib

@pytest.mark.parametrize('fuse', [None, True, False])
def test_mount(fuse):
  from ufs.impl.mapper import Mapper
  from ufs.impl.local import Local
  from ufs.impl.prefix import Prefix
  from ufs.access.mount import mount
  with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = pathlib.Path(tmpdir)
    (tmpdir / 'test').write_text('hello world!')
    (tmpdir / 'output').mkdir()
    (tmpdir / 'output' / 'test2').write_text('hello world!')
    with mount(Mapper({
      '/': Prefix(Local(), tmpdir/'output'),
      'test': Prefix(Local(), tmpdir/'test'),
    }), fuse=fuse) as mnt:
      assert (mnt/'test').read_text() == 'hello world!'
      assert (mnt/'test2').read_text() == 'hello world!'
      (mnt/'test2').write_text('hi')
      (mnt/'test3').write_text('hi')
    assert not mnt.exists()
    assert (tmpdir/'output'/'test2').read_text() == 'hi'
    assert (tmpdir/'output'/'test3').read_text() == 'hi'

@pytest.mark.parametrize('fuse', [None, True, False])
@pytest.mark.asyncio
async def test_async_mount(fuse):
  from ufs.impl.mapper import Mapper
  from ufs.impl.local import Local
  from ufs.impl.prefix import Prefix
  from ufs.access.mount import async_mount
  with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = pathlib.Path(tmpdir)
    (tmpdir / 'test').write_text('hello world!')
    (tmpdir / 'output').mkdir()
    (tmpdir / 'output' / 'test2').write_text('hello world!')
    async with async_mount(Mapper({
      '/': Prefix(Local(), tmpdir/'output'),
      'test': Prefix(Local(), tmpdir/'test'),
    }), fuse=fuse) as mnt:
      assert (mnt/'test').read_text() == 'hello world!'
      assert (mnt/'test2').read_text() == 'hello world!'
      (mnt/'test2').write_text('hi')
      (mnt/'test3').write_text('hi')
    assert not mnt.exists()
    assert (tmpdir/'output'/'test2').read_text() == 'hi'
    assert (tmpdir/'output'/'test3').read_text() == 'hi'
