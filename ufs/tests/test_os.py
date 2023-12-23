import pytest
from ufs.spec import UFS

from ufs.tests.fixtures import ufs
def test_os(ufs: UFS):
  from ufs.access.os import UOS
  os = UOS(ufs)
  assert os.access('/', 511)
  os.mkdir('/test')
  assert os.listdir('/') == ['test']
  os.rmdir('/test')
  assert os.listdir('/') == []
  with pytest.raises(FileNotFoundError): os.stat('/test')
