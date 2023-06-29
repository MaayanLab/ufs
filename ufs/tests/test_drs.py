import pytest

@pytest.fixture
def ufs():
  import tempfile
  from ufs.impl.local import Local
  from ufs.impl.prefix import Prefix
  from ufs.access.pathlib import UPath
  with tempfile.TemporaryDirectory() as tmpdir:
    with Prefix(Local(), tmpdir) as ufs:
      upath = UPath(ufs)
      (upath/'a').write_text('Hello World!')
      (upath/'b').mkdir()
      (upath/'b'/'c').write_text('Hello')
      (upath/'b'/'d').write_text('World')
      yield ufs

@pytest.fixture
def drs_server(ufs):
  from ufs.utils.socket import autosocket
  from ufs.access.drs import serve_ufs_via_drs
  host, port = autosocket()
  with serve_ufs_via_drs(ufs, host, port):
    yield f"{host}:{port}"

@pytest.fixture
def drs_client(drs_server):
  from ufs.impl.drs import DRS
  from ufs.impl.prefix import Prefix
  with Prefix(DRS(scheme='http'), drs_server) as drs:
    yield drs

def test_drs(ufs, drs_client):
  from ufs.access.drs import index_ufs_for_drs
  from ufs.access.pathlib import UPath
  index = index_ufs_for_drs(ufs)
  drs = UPath(drs_client)
  assert (drs / index['sha256sums']['/']).is_dir()
  assert (drs / index['sha256sums']['/a']).read_text() == 'Hello World!'
  assert (drs / index['sha256sums']['/b']).is_dir()
  assert (drs / index['sha256sums']['/b/c']).is_file()
  assert (drs / index['sha256sums']['/b/d']).is_file()
  assert {p.name for p in (drs / index['sha256sums']['/'] / 'b').iterdir()} == {'c', 'd'}
  with pytest.raises(FileNotFoundError): (drs/'nowhere').read_text()
