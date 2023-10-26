import pytest
import pathlib
from ufs.spec import UFS
from ufs.access.pathlib import UPath
from ufs.utils.pathlib import SafePurePosixPath

@pytest.fixture(params=[
  p.stem
  for p in (pathlib.Path(__file__).parent / 'fixtures').glob('[!_]*.py')
])
def ufs(request):
  ''' Load different ufs implementations from fixtures directory to be tested uniformly
  '''
  import importlib
  yield from importlib.import_module(f"ufs.tests.fixtures.{request.param}").ufs()

def test_os(ufs: UFS):
  from ufs.access.os import UOS
  os = UOS(ufs)
  assert os.access('/', 511)
  os.mkdir('/test')
  assert os.listdir('/') == ['test']
  os.rmdir('/test')
  assert os.listdir('/') == []
  with pytest.raises(FileNotFoundError): os.stat('/test')

def test_map(ufs: UFS):
  from ufs.access.map import UMap
  M = UMap(ufs)
  M['a'] = 'b'
  M['c'] = {
    'd': 'e',
    'f': {'g': 'h'},
  }
  assert M['c']['f']['g'] == 'h'
  del M['c']
  assert list(M) == ['a']

@pytest.fixture(params=[
  'pathlib',
  'fuse',
  'ffuse',
])
def path(request, ufs):
  ufs.info(SafePurePosixPath())
  if request.param == 'pathlib':
    upath = UPath(ufs) / 'pathlib'
    upath.mkdir()
    yield upath
  elif request.param == 'fuse':
    from ufs.access.fuse import fuse_mount
    with fuse_mount(ufs) as mount_dir:
      mount_dir = mount_dir / 'fuse'
      mount_dir.mkdir()
      yield mount_dir
  elif request.param == 'ffuse':
    from ufs.access.ffuse import ffuse_mount
    with ffuse_mount(ufs) as mount_dir:
      mount_dir = mount_dir / 'ffuse'
      mount_dir.mkdir()
      yield mount_dir


def test_ufs(path: UPath):
  ''' Actually test that filesystem ops work as expected
  '''
  assert path.is_dir()
  (path/'A').write_text('Hello World!')
  assert (path/'A').is_file()
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

def test_overlay():
  from ufs.impl.memory import Memory
  from ufs.impl.overlay import Overlay
  from ufs.access.pathlib import UPath
  lower = Memory()
  upper = Memory()
  with Overlay(lower, upper) as overlay:
    lower = UPath(lower)
    upper = UPath(upper)
    overlay = UPath(overlay)
    (overlay / 'test_dir').mkdir()
    (lower / 'test').write_text('Hello World')
    assert {p.name for p in overlay.iterdir()} == {'test', 'test_dir'}
    (overlay / 'test2').write_text('Hello World!')
    assert {p.name for p in lower.iterdir()} == {'test'}
    assert {p.name for p in upper.iterdir()} == {'test2', 'test_dir'}
    assert {p.name for p in overlay.iterdir()} == {'test', 'test2', 'test_dir'}
    (overlay / 'test').write_text('Hello World!')
    assert (lower / 'test').read_text() == 'Hello World'
    assert (overlay / 'test').read_text() == 'Hello World!'

def test_mapper():
  from ufs.impl.memory import Memory
  from ufs.impl.mapper import Mapper
  from ufs.access.pathlib import UPath
  root = Memory()
  sub = Memory()
  with Mapper({ '/': root, '/a/b': sub }) as mapper:
    root = UPath(root)
    (root/'a/b').mkdir(parents=True) # TODO
    sub = UPath(sub)
    mapper = UPath(mapper)
    assert {p.name for p in (mapper/'a').iterdir()} == {'b'}
    (mapper/'a/b/c').write_text('test2')
    (mapper/'a/d').write_text('test1')
    assert (sub/'c').read_text() == 'test2'
    assert (root/'a/d').read_text() == 'test1'

def test_singlefile_mapper():
  from ufs.impl.memory import Memory
  from ufs.impl.mapper import Mapper
  from ufs.impl.prefix import Prefix
  from ufs.access.pathlib import UPath
  backing = Memory()
  ufs = Mapper({ 'A': Prefix(backing, '/A') })
  with ufs:
    (UPath(backing) / 'A').write_text('a')
    (UPath(backing) / 'B').write_text('b')
    assert [p.name for p in UPath(ufs).iterdir()] == ['A']
    assert (UPath(ufs)/'A').read_text() == 'a'
