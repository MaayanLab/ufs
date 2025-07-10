import pytest
import shutil
from ufs.spec import UFS, AccessScope
from ufs.impl.sync import Sync

from ufs.tests.fixtures import ufs
@pytest.mark.parametrize('fuse', [True, False])
def test_mountable(ufs: UFS, fuse):
  from ufs.access.mount import mount
  from ufs.access.pathlib import UPath
  if fuse and ufs.scope().value < AccessScope.system.value:
    pytest.skip('UFS store access scope not compatible with fuse')
  with mount(Sync(ufs), fuse=fuse) as mount_dir:
    # verify we can do things in the mount
    path = mount_dir / ('fuse' if fuse else 'ffuse')
    path.mkdir()
    assert path.is_dir()
    (path/'A').write_text('Hello World!')
    assert (path/'A').is_file()
    # can we seek/write files correctly
    with (path/'A').open('r+') as fh:
      assert fh.read() == 'Hello World!'
      fh.seek(0)
      fh.write('h')
      fh.seek(6)
      fh.write('w')
      fh.seek(0)
      assert fh.read() == 'hello world!'
    # does rename work
    (path/'A').rename(path/'B')
    # does append work
    with (path/'B').open('a') as fa:
      fa.write('\n!')
    with pytest.raises(FileNotFoundError): (path/'A').read_text()
    # native shutil copyfile should work
    shutil.copyfile(path/'B', path/'A')
    # is the content the same as what we wrote after the rename?
    with (path/'B').open('r') as fr:
      fr = iter(fr)
      assert next(fr) == 'hello world!\n'
      assert next(fr) == '!'
      with pytest.raises(StopIteration): next(fr)
    assert {p.name for p in path.iterdir()} == {'A', 'B'}
    # can we remove a file?
    (path/'B').unlink()
    assert not (path/'B').exists()
  # make sure the changes have also propagated to the underlying store
  upath = UPath(ufs)/ ('fuse' if fuse else 'ffuse')
  assert {p.name for p in upath.iterdir()} == {'A'}
  assert (upath/'A').read_text() == 'hello world!\n!'
