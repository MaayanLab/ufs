import pytest

def ufs():
  try:
    import paramiko
  except ImportError:
    pytest.skip('paramiko not available')
  else:
    from ufs.impl.memory import Memory
    from ufs.impl.sftp import SFTP
    from ufs.access.sftp import serve_ufs_via_sftp
    from ufs.utils.socket import autosocket
    # find a free port to run sftp
    host, port = autosocket()
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
