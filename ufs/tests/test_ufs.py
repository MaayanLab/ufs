import pytest
from ufs.spec import UFS
from ufs.pathlib import UPath
from ufs.utils.pathlib import SafePurePosixPath

@pytest.fixture(params=[
  'local',
  'memory',
  'process-memory',
  'local-memory-writecache',
  'memory-async-sync',
  'fsspec-local',
  'fsspec-memory',
  'dircache-local',
  's3',
  'sbfs',
])
def ufs(request):
  ''' Result in various UFS implementations
  '''
  from ufs.impl.prefix import Prefix
  if request.param == 'local':
    import tempfile
    from ufs.impl.local import Local
    with tempfile.TemporaryDirectory() as tmp:
      yield Prefix(Local(), tmp)
  elif request.param == 'local-memory-writecache':
    import tempfile
    from ufs.impl.local import Local
    from ufs.impl.memory import Memory
    from ufs.impl.writecache import Writecache
    with tempfile.TemporaryDirectory() as tmp:
      yield Writecache(Prefix(Local(), tmp), Memory())
  elif request.param == 'memory':
    from ufs.impl.memory import Memory
    yield Memory()
  elif request.param == 'process-memory':
    from ufs.impl.process import Process
    from ufs.impl.memory import Memory
    ufs = Process(Memory())
    ufs.start()
    yield ufs
    ufs.stop()
  elif request.param == 'memory-async-sync':
    from ufs.impl.sync import Sync
    from ufs.impl.asyn import Async
    from ufs.impl.memory import Memory
    ufs = Sync(Async(Memory()))
    ufs.start()
    yield ufs
    ufs.stop()
  elif request.param == 'fsspec-local':
    import tempfile
    from ufs.impl.fsspec import FSSpec
    from fsspec.implementations.local import LocalFileSystem
    with tempfile.TemporaryDirectory() as tmp:
      yield Prefix(FSSpec(LocalFileSystem()), tmp)
  elif request.param == 'fsspec-memory':
    from ufs.impl.fsspec import FSSpec
    from fsspec.implementations.memory import MemoryFileSystem
    yield FSSpec(MemoryFileSystem())
  elif request.param == 'dircache-local':
    import tempfile
    from ufs.impl.local import Local
    from ufs.impl.dircache import DirCache
    with tempfile.TemporaryDirectory() as tmp:
      yield DirCache(Prefix(Local(), tmp))
  elif request.param == 's3':
    import shutil
    # look for the minio command for running an s3 server
    minio = shutil.which('minio')
    if minio is None: pytest.skip('minio binary not available')
    else:
      import os
      import sys
      import uuid
      import socket
      import tempfile
      import functools
      from urllib.request import Request, urlopen
      from subprocess import Popen
      from ufs.impl.s3 import S3
      from ufs.utils.polling import wait_for, safe_predicate
      from ufs.utils.process import active_process
      # get a temporary directory to store s3 files in
      with tempfile.TemporaryDirectory() as tmp:
        # generate credentials for minio
        MINIO_ROOT_USER, MINIO_ROOT_PASSWORD = str(uuid.uuid4()), str(uuid.uuid4())
        # find a free port to run minio
        with socket.socket() as s:
          s.bind(('', 0))
          host, port = s.getsockname()
        # actually run minio
        with active_process(Popen(
          [minio, 'server', tmp, '--address', f"{host}:{port}"],
          env=dict(
            os.environ,
            MINIO_ROOT_USER=MINIO_ROOT_USER,
            MINIO_ROOT_PASSWORD=MINIO_ROOT_PASSWORD,
          ),
          stderr=sys.stderr,
          stdout=sys.stdout,
        )):
          # wait for minio to be running & ready
          wait_for(functools.partial(safe_predicate, lambda: urlopen(Request(f"http://{host}:{port}/minio/health/live", method='HEAD')).status == 200))
          # create an fsspec connection to the minio server
          ufs = Prefix(
            S3(
              access_key=MINIO_ROOT_USER,
              secret_access_key=MINIO_ROOT_PASSWORD,
              endpoint_url=f"http://{host}:{port}",
            ),
            '/test',
          )
          ufs.mkdir('/')
          yield ufs
  elif request.param == 'sbfs':
    import os
    import uuid
    from ufs.impl.sync import Sync
    from ufs.impl.process import Process
    from ufs.impl.sbfs import SBFS
    from ufs.impl.prefix import Prefix
    from ufs.impl.memory import Memory
    from ufs.impl.writecache import Writecache
    from ufs.shutil import rmtree
    try:
      ufs = Writecache(
        Prefix(
          Sync(SBFS(os.environ['SBFS_AUTH_TOKEN'])),
          SafePurePosixPath(os.environ['SBFS_PREFIX'])/str(uuid.uuid4())
        ),
        Memory()
      )
      ufs.mkdir('/')
      try:
        yield ufs
      finally:
        rmtree(ufs, '/')
    except KeyError:
      pytest.skip('Environment variables SBFS_AUTH_TOKEN and SBFS_PREFIX required for sbfs')

def test_os(ufs: UFS):
  from ufs.os import UOS
  os = UOS(ufs)
  assert os.access('/', 511)
  os.mkdir('/test')
  assert os.listdir('/') == ['test']
  os.rmdir('/test')
  assert os.listdir('/') == []
  with pytest.raises(FileNotFoundError): os.lstat('/test')

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
  ufs.info(SafePurePosixPath('/'))
  if request.param == 'pathlib':
    upath = UPath(ufs) / 'pathlib'
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
    fa.write('!')
  with pytest.raises(FileNotFoundError): (path/'A').read_text()
  assert (path/'B').read_text() == 'hello world!!'
  assert [p.name for p in path.iterdir()] == ['B']
  (path/'B').unlink()
  assert not (path/'B').exists()
