import pytest

def ufs():
  import shutil
  # psycopg2 is necessary for this functionality
  try:
    import psycopg2
  except ImportError:
    pytest.skip('psycopg2 not available')
    return
  # look for the docker command for running a postgres database
  docker = shutil.which('docker')
  if docker is None:
    pytest.skip('docker binary not available')
    return
  import sys
  import uuid
  import socket
  import functools
  from subprocess import Popen
  from ufs.impl.pg import Postgres
  from ufs.impl.memory import Memory
  from ufs.impl.writecache import Writecache
  from ufs.utils.process import active_process
  from ufs.utils.polling import wait_for, safe_predicate
  # generate credentials for postgres
  POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD = 'postgres', 'postgres', str(uuid.uuid4())
  # find a free port to run postgres
  with socket.socket() as s:
    s.bind(('', 0))
    host, port = s.getsockname()
  # actually run postgres
  with active_process(Popen(
    [
      docker, 'run',
      '-e', f"POSTGRES_DB={POSTGRES_DB}",
      '-e', f"POSTGRES_USER={POSTGRES_USER}",
      '-e', f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
      '-p', f"{port}:5432",
      '-i', 'postgres'
    ],
    stderr=sys.stderr,
    stdout=sys.stdout,
  )):
    database_url = f"postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{port}/{POSTGRES_DB}"
    # wait for postgres to be running & ready
    wait_for(functools.partial(safe_predicate, lambda: psycopg2.connect(database_url).close() or True))
    # create the connection
    with Writecache(
      Postgres(database_url),
      Memory()
    ) as ufs:
      yield ufs
