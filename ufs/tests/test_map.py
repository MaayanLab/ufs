from ufs.spec import UFS

from ufs.tests.fixtures import ufs
def test_map(ufs: UFS):
  from ufs.access.map import UMap
  M = UMap(ufs)
  M['a'] = 'b'
  M['c'] = {
    'd': 'e',
    'f': {'g': 'h'},
  }
  assert M['c']['f']['g'] == 'h'
  assert sorted(M['c']) == ['d', 'f']
  del M['c']
  assert list(M) == ['a']
