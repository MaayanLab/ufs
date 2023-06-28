import pytest
from ufs.spec import UFS

try: import paramiko
except ImportError: paramiko = None

@pytest.fixture
def ufs():
  from ufs.impl.logger import Logger
  from ufs.impl.memory import Memory
  with Logger(Memory()) as ufs:
    yield ufs

@pytest.fixture
def sftp_server(ufs: UFS):
  if paramiko is None: pytest.skip(f"Install paramiko for sftp support")
  else:
    import os
    import sys
    import json
    from subprocess import Popen
    from ufs.utils.process import active_process
    from ufs.utils.polling import wait_for
    from ufs.utils.socket import nc_z, autosocket
    # find a free port to run sftp
    host, port = autosocket()
    username, password = 'admin', 'admin'
    opts = {
      'ufs': ufs.to_dict(),
      'host': host,
      'port': port,
      'username': username,
      'password': password,
    }
    with active_process(Popen(
      [sys.executable, '-m', 'ufs.access.sftp', json.dumps(opts)],
      env=os.environ,
      stderr=sys.stderr,
      stdout=sys.stdout,
    )):
      wait_for(lambda: nc_z(host, port))
      yield opts

import contextlib
@contextlib.contextmanager
def sftp_client(sftp_server):
  ssh = paramiko.SSHClient()
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  ssh.connect(
    sftp_server['host'],
    port=sftp_server['port'],
    username=sftp_server['username'],
    password=sftp_server['password'],
    look_for_keys=False,
  )
  try:
    sftp = ssh.open_sftp()
    try:
      yield sftp
    finally:
      sftp.close()
  finally:
    ssh.close()

def test_sftp(sftp_server: paramiko.SFTPServer):
  with sftp_client(sftp_server) as client1:
    with sftp_client(sftp_server) as client2:
      assert client1.listdir() == []
      with client2.open('test', 'wb') as fw:
        fw.write(b'hello world!')
      assert client2.listdir() == ['test']
      with client1.open('test', 'rb') as fr:
        assert fr.read() == b'hello world!'
