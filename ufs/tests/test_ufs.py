import pytest
from ufs.spec import UFS
from ufs.access.pathlib import UPath
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
  'rclone-local',
  'ftp',
  'sftp',
])
def ufs(request):
  ''' Result in various UFS implementations
  '''
  from ufs.impl.prefix import Prefix
  if request.param == 'local':
    import tempfile
    from ufs.impl.local import Local
    with tempfile.TemporaryDirectory() as tmp:
      with Prefix(Local(), tmp) as ufs:
        yield ufs
  elif request.param == 'local-memory-writecache':
    import tempfile
    from ufs.impl.local import Local
    from ufs.impl.memory import Memory
    from ufs.impl.writecache import Writecache
    with tempfile.TemporaryDirectory() as tmp:
      with Writecache(Prefix(Local(), tmp), Memory()) as ufs:
        yield ufs
  elif request.param == 'memory':
    from ufs.impl.memory import Memory
    with Memory() as ufs:
      yield ufs
  elif request.param == 'process-memory':
    from ufs.impl.process import Process
    from ufs.impl.memory import Memory
    with Process(Memory()) as ufs:
      yield ufs
  elif request.param == 'memory-async-sync':
    from ufs.impl.sync import Sync
    from ufs.impl.asyn import Async
    from ufs.impl.memory import Memory
    with Sync(Async(Memory())) as ufs:
      yield ufs
  elif request.param == 'fsspec-local':
    import tempfile
    from ufs.impl.fsspec import FSSpec
    from fsspec.implementations.local import LocalFileSystem
    with tempfile.TemporaryDirectory() as tmp:
      with Prefix(FSSpec(LocalFileSystem()), tmp) as ufs:
        yield ufs
  elif request.param == 'fsspec-memory':
    from ufs.impl.fsspec import FSSpec
    from fsspec.implementations.memory import MemoryFileSystem
    with FSSpec(MemoryFileSystem()) as ufs:
      yield ufs
  elif request.param == 'dircache-local':
    import tempfile
    from ufs.impl.local import Local
    from ufs.impl.dircache import DirCache
    with tempfile.TemporaryDirectory() as tmp:
      with DirCache(Prefix(Local(), tmp)) as ufs:
        yield ufs
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
      from ufs.access.shutil import rmtree
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
          with Prefix(
            S3(
              access_key=MINIO_ROOT_USER,
              secret_access_key=MINIO_ROOT_PASSWORD,
              endpoint_url=f"http://{host}:{port}",
            ),
            '/test',
          ) as ufs:
            ufs.mkdir('/')
            try:
              yield ufs
            finally:
              rmtree(ufs, '/')
  elif request.param == 'sbfs':
    import os
    import uuid
    from ufs.impl.sync import Sync
    from ufs.impl.process import Process
    from ufs.impl.sbfs import SBFS
    from ufs.impl.prefix import Prefix
    from ufs.impl.memory import Memory
    from ufs.impl.writecache import Writecache
    from ufs.access.shutil import rmtree
    try:
      with Writecache(
        Prefix(
          Sync(SBFS(os.environ['SBFS_AUTH_TOKEN'])),
          SafePurePosixPath(os.environ['SBFS_PREFIX'])/str(uuid.uuid4())
        ),
        Memory()
      ) as ufs:
        ufs.mkdir('/')
        try:
          yield ufs
        finally:
          rmtree(ufs, '/')
    except KeyError:
      pytest.skip('Environment variables SBFS_AUTH_TOKEN and SBFS_PREFIX required for sbfs')
  elif request.param == 'rclone-local':
    import pathlib
    import tempfile
    from ufs.impl.rclone import RClone
    from ufs.impl.prefix import Prefix
    from ufs.impl.writecache import Writecache
    from ufs.impl.memory import Memory
    from ufs.impl.logger import Logger
    with tempfile.TemporaryDirectory() as tmp:
      tmp = pathlib.Path(tmp)
      (tmp/'data').mkdir()
      (tmp/'config').mkdir()
      (tmp/'config'/'rclone.conf').touch()
      with Writecache(Prefix(Logger(RClone(dict(
        RCLONE_CONFIG_DIR=str(tmp/'config'),
        RCLONE_CONFIG_LOCAL_TYPE='alias',
        RCLONE_CONFIG_LOCAL_REMOTE=str(tmp/'data'),
      ))), '/local'), Memory()) as ufs:
        yield ufs
  elif request.param == 'ftp':
    import uuid
    import shutil
    from ufs.impl.ftp import FTP
    from ufs.impl.prefix import Prefix
    from ufs.access.shutil import rmtree
    try:
      import pyftpdlib
    except ImportError:
      pytest.skip('pyftpdlib not available')
    else:
      import os
      import sys
      import uuid
      import socket
      import tempfile
      import functools
      from urllib.request import Request, urlopen
      from subprocess import Popen
      from ufs.impl.ftp import FTP
      from ufs.impl.memory import Memory
      from ufs.impl.writecache import Writecache
      from ufs.utils.polling import wait_for, safe_predicate
      from ufs.utils.process import active_process
      from ufs.access.shutil import rmtree
      def nc_z(host, port, timeout=1):
        with socket.create_connection((host, port), timeout=timeout):
          return True
      # get a temporary directory to store ftp files in
      with tempfile.TemporaryDirectory() as tmp:
        # generate credentials for ftp
        ftp_user, ftp_passwd = str(uuid.uuid4()), str(uuid.uuid4())
        # find a free port to run ftp
        with socket.socket() as s:
          s.bind(('', 0))
          host, port = s.getsockname()
        # actually run ftp server
        with active_process(Popen(
          [sys.executable, '-m', 'pyftpdlib', f"--port={port}", f"--username={ftp_user}", f"--password={ftp_passwd}", f"--directory={tmp}", '--write'],
          env=os.environ,
          stderr=sys.stderr,
          stdout=sys.stdout,
        )):
          # wait for ftp to be running & ready
          wait_for(functools.partial(safe_predicate, lambda: nc_z(host, port)))
          # create an fsspec connection to the minio server
          with Writecache(FTP(
            host=host,
            user=ftp_user,
            passwd=ftp_passwd,
            port=port,
          ), Memory()) as ufs:
            yield ufs
  elif request.param == 'sftp':
    try:
      import paramiko
    except ImportError:
      pytest.skip('paramiko not available')
    else:
      import os
      import sys
      import socket
      import functools
      from subprocess import Popen
      from ufs.impl.memory import Memory
      from ufs.impl.sftp import SFTP
      from ufs.access.sftp import serve_ufs_via_sftp
      # find a free port to run sftp
      with socket.socket() as s:
        s.bind(('', 0))
        host, port = s.getsockname()
      username, password = 'admin', 'admin'
      with Memory() as ufs:
        with serve_ufs_via_sftp(
          ufs=ufs,
          host=host,
          port=port,
          username=username,
          password=password,
        ):
          with SFTP(
            host=host,
            port=port,
            username=username,
            password=password,
          ) as ufs:
            yield ufs

def test_os(ufs: UFS):
  from ufs.access.os import UOS
  os = UOS(ufs)
  assert os.access('/', 511)
  os.mkdir('/test')
  assert os.listdir('/') == ['test']
  os.rmdir('/test')
  assert os.listdir('/') == []
  with pytest.raises(FileNotFoundError): os.stat('/test')

def test_map(ufs: UFS):
  from ufs.access.map import UMap
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
  'ffuse',
])
def path(request, ufs):
  ufs.info(SafePurePosixPath('/'))
  if request.param == 'pathlib':
    upath = UPath(ufs) / 'pathlib'
    upath.mkdir()
    yield upath
  elif request.param == 'fuse':
    from ufs.access.fuse import fuse_mount
    with fuse_mount(ufs) as mount_dir:
      mount_dir = mount_dir / 'fuse'
      mount_dir.mkdir()
      yield mount_dir
  elif request.param == 'ffuse':
    from ufs.access.ffuse import ffuse_mount
    with ffuse_mount(ufs) as mount_dir:
      mount_dir = mount_dir / 'ffuse'
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
