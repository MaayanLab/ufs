import pytest
from ufs.access.pathlib import UPath
from ufs.access.shutil import rmtree
from ufs.utils.pathlib import SafePurePosixPath

from ufs.tests.fixtures import ufs
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
    rmtree(upath, SafePurePosixPath())
  elif request.param == 'fuse':
    from ufs.access.fuse import fuse_mount
    with fuse_mount(ufs) as mount_dir:
      mount_dir = mount_dir / 'fuse'
      mount_dir.mkdir()
      yield mount_dir
      mount_dir.rmdir()
  elif request.param == 'ffuse':
    from ufs.access.ffuse import ffuse_mount
    with ffuse_mount(ufs) as mount_dir:
      mount_dir = mount_dir / 'ffuse'
      mount_dir.mkdir()
      yield mount_dir
      mount_dir.rmdir()

def test_path(path: UPath):
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
