import pytest
from ufs.spec import UFS
from ufs.pathlib import UPath

@pytest.fixture(params=[
  'local',
  'memory',
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

@pytest.fixture(params=[
  'pathlib',
])
def path(request, ufs):
  if request.param == 'pathlib':
    yield UPath(ufs)

def test_ufs(path: UPath):
  ''' Actually test that filesystem ops work as expected
  '''
  assert path.exists()
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
  assert next(iter(path.iterdir())).name == 'B'
  (path/'B').unlink()
  assert not (path/'B').exists()
