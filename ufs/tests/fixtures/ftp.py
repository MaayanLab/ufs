import pytest

def ufs():
  import uuid
  try:
    import pyftpdlib
  except ImportError:
    pytest.skip('pyftpdlib not available')
  else:
    import os
    import sys
    import uuid
    import tempfile
    from subprocess import Popen
    from ufs.impl.ftp import FTP
    from ufs.impl.memory import Memory
    from ufs.impl.writecache import Writecache
    from ufs.utils.polling import wait_for
    from ufs.utils.process import active_process
    from ufs.utils.socket import nc_z, autosocket
    # get a temporary directory to store ftp files in
    with tempfile.TemporaryDirectory() as tmp:
      # generate credentials for ftp
      ftp_user, ftp_passwd = str(uuid.uuid4()), str(uuid.uuid4())
      # find a free port to run ftp
      host, port = autosocket()
      # actually run ftp server
      with active_process(Popen(
        [sys.executable, '-m', 'pyftpdlib', f"--port={port}", f"--username={ftp_user}", f"--password={ftp_passwd}", f"--directory={tmp}", '--write'],
        env=os.environ,
        stderr=sys.stderr,
        stdout=sys.stdout,
      )):
        # wait for ftp to be running & ready
        wait_for(lambda: nc_z(host, port))
        # create an fsspec connection to the minio server
        with Writecache(FTP(
          host=host,
          user=ftp_user,
          passwd=ftp_passwd,
          port=port,
        ), Memory()) as ufs:
          yield ufs
