import pytest

def ufs():
  import shutil
  # look for the docker command for running an s3 server
  docker = shutil.which('docker')
  if docker is None:
    pytest.skip('docker binary not available')
    return
  import sys
  from subprocess import check_call
  if check_call([
    docker, 'pull', 'minio/minio'], 
    stderr=sys.stderr,
    stdout=sys.stdout,
  ) != 0:
    pytest.skip('dockerized minio not available')
    return
  import sys
  import uuid
  import socket
  import functools
  from urllib.request import Request, urlopen
  from subprocess import Popen
  from ufs.impl.s3fs import S3
  from ufs.impl.prefix import Prefix
  from ufs.impl.memory import Memory
  from ufs.impl.writecache import Writecache
  from ufs.utils.polling import wait_for, safe_predicate
  from ufs.utils.process import active_process
  from ufs.utils.pathlib import SafePurePosixPath
  from ufs.access.shutil import rmtree
  # generate credentials for minio
  MINIO_ROOT_USER, MINIO_ROOT_PASSWORD = str(uuid.uuid4()), str(uuid.uuid4())
  # find a free port to run minio
  with socket.socket() as s:
    s.bind(('', 0))
    host, port = s.getsockname()
  # actually run minio
  with active_process(Popen(
    [
      docker, 'run',
      '-e', f"MINIO_ROOT_USER={MINIO_ROOT_USER}",
      '-e', f"MINIO_ROOT_PASSWORD={MINIO_ROOT_PASSWORD}",
      '-p', f"{port}:9000",
      '-i', 'minio/minio',
      'server', '/data'
    ],
    stderr=sys.stderr,
    stdout=sys.stdout,
  )):
    # wait for minio to be running & ready
    wait_for(functools.partial(safe_predicate, lambda: urlopen(Request(f"http://localhost:{port}/minio/health/live", method='HEAD')).status == 200))
    # create an fsspec connection to the minio server
    with Writecache(Prefix(
      S3(
        access_key=MINIO_ROOT_USER,
        secret_access_key=MINIO_ROOT_PASSWORD,
        endpoint_url=f"http://localhost:{port}",
        ttl=0, # TTL >0 won't work when modifying from other processes
      ),
      '/storage',
    ), Memory()) as ufs:
      ufs.mkdir(SafePurePosixPath())
      try:
        yield ufs
      finally:
        rmtree(ufs, SafePurePosixPath())
