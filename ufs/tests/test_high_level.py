import typing as t
from ufs.spec import SyncUFS
from ufs.impl.sync import Sync

from ufs.tests.fixtures import ufs
def test_h5py_file_object(ufs: SyncUFS):
  import h5py
  import numpy as np
  from ufs.access.pathlib import UPath
  X = np.random.rand(1000, 1000)
  with (UPath(ufs)/'f.h5').open('wb') as fh:
    f = h5py.File(fh, 'w')
    f['X'] = X
    f.close()
  with (UPath(ufs)/'f.h5').open('rb+') as fh:
    f = h5py.File(fh, 'r+')
    d = t.cast(h5py.Dataset, f['X'])
    d[0, 0] = -1
    f.close()
  with (UPath(ufs)/'f.h5').open('rb') as fh:
    f = h5py.File(fh, 'r')
    d = t.cast(h5py.Dataset, f['X'])
    try:
      assert d[0,0] == -1
      d[-100:, -100:]
      d[0:100, 0:100]
    finally:
      f.close()

def test_h5py_in_mount(ufs: SyncUFS):
  import h5py
  import numpy as np
  from ufs.access.mount import mount
  X = np.random.rand(1000, 1000)
  with mount(ufs) as mount_dir:
    f = h5py.File(mount_dir/'f.h5', 'w')
    f['X'] = X
    f.close()
    f = h5py.File(mount_dir/'f.h5', 'r+')
    d = t.cast(h5py.Dataset, f['X'])
    d[0, 0] = -1
    f.close()
    f = h5py.File(mount_dir/'f.h5', 'r')
    d = t.cast(h5py.Dataset, f['X'])
    try:
      assert d[0,0] == -1
      d[-100:, -100:]
      d[0:100, 0:100]
    finally:
      f.close()
