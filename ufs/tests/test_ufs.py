import pytest
from ufs.spec import UFS
from ufs.pathlib import UPath

@pytest.fixture(params=[
  'local',
  'memory',
  'fsspec-local',
  'fsspec-memory',
  'dircache-local',
])
def ufs(request):
  ''' Result in various UFS implementations
  '''
  from ufs.impl.prefix import Prefix
  if request.param == 'local':
    import tempfile
    from ufs.impl.local import Local
    with tempfile.TemporaryDirectory() as tmp:
      yield Prefix(Local(), tmp+'/')
  elif request.param == 'memory':
    from ufs.impl.memory import Memory
    yield Prefix(Memory())
  elif request.param == 'fsspec-local':
    import tempfile
    from ufs.impl.fsspec import FSSpec
    from fsspec.implementations.local import LocalFileSystem
    with tempfile.TemporaryDirectory() as tmp:
      yield Prefix(FSSpec(LocalFileSystem()), tmp+'/')
  elif request.param == 'fsspec-memory':
    from ufs.impl.fsspec import FSSpec
    from fsspec.implementations.memory import MemoryFileSystem
    yield Prefix(FSSpec(MemoryFileSystem()))
  elif request.param == 'dircache-local':
    import tempfile
    from ufs.impl.local import Local
    from ufs.impl.dircache import DirCache
    with tempfile.TemporaryDirectory() as tmp:
      yield Prefix(DirCache(Local()), tmp+'/')

def test_map(ufs: UFS):
  from ufs.map import UMap
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
])
def path(request, ufs):
  if request.param == 'pathlib':
    upath = UPath(ufs)
    if not upath.exists(): upath.mkdir()
    upath = upath / 'pathlib'
    upath.mkdir()
    yield upath
  elif request.param == 'fuse':
    from ufs.fuse import fuse_mount
    with fuse_mount(ufs) as mount_dir:
      mount_dir = mount_dir / 'fuse'
      mount_dir.mkdir()
      yield mount_dir

def test_ufs(path: UPath):
  ''' Actually test that filesystem ops work as expected
  '''
  (path/'A').write_text('Hello World!')
  assert (path/'A').exists()
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
    fa.write('!')
  with pytest.raises(FileNotFoundError): (path/'A').read_text()
  assert (path/'B').read_text() == 'hello world!!'
  assert [p.name for p in path.iterdir()] == ['B']
  (path/'B').unlink()
  assert not (path/'B').exists()
