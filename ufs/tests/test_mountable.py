import pytest
from ufs.spec import UFS, AccessScope

from ufs.tests.fixtures import ufs

@pytest.mark.parametrize('fuse', [True, False])
def test_mountable(ufs: UFS, fuse):
  from ufs.access.mount import mount
  from ufs.access.pathlib import UPath
  if fuse and ufs.scope().value < AccessScope.system.value:
    pytest.skip('UFS store access scope not compatible with fuse')
  with mount(ufs, fuse=fuse) as mount_dir:
    (mount_dir/'test').mkdir(parents=True)
    with (mount_dir/'test'/'a').open('w+') as fw:
      fw.write('Hello World!')
      fw.seek(0)
      fw.write('Hi')
    import time; time.sleep(0.5)
  upath = UPath(ufs)
  assert [p.name for p in upath.iterdir()] == ['test']
  assert (upath/'test').is_dir()
  assert (upath/'test'/'a').read_text() == 'Hillo World!'
