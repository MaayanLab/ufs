''' Serve a UFS over SFTP
'''
import os
import errno
import paramiko, paramiko.common
import pathlib
import contextlib
import logging
import traceback
import typing as t

from ufs.access.os import UOS
from ufs.spec import SyncUFS
from ufs.utils.pathlib import pathname

logger = logging.getLogger(__name__)

class USSHServer(paramiko.ServerInterface):
  def __init__(self, ufs: SyncUFS, username: str, password: t.Optional[str] = None):
    super().__init__()
    self._ufs = ufs
    self._uos = UOS(self._ufs)
    self._username = username
    self._password = password

  def check_channel_request(self, kind, chanid):
    return paramiko.common.OPEN_SUCCEEDED

  def get_allowed_auths(self, username):
    if self._password == None:
      return 'none'
    else:
      return 'password'

  def check_auth_none(self, username):
    if username == self._username and self._password is None:
      return paramiko.common.AUTH_SUCCESSFUL
    else:
      return paramiko.common.AUTH_FAILED

  def check_auth_password(self, username, password):
    if username == self._username and password == self._password:
      return paramiko.common.AUTH_SUCCESSFUL
    else:
      return paramiko.common.AUTH_FAILED

class USFTPHandle(paramiko.SFTPHandle):
  def __init__(self, fd: int, path: str, server: 'USFTPServer', flags: int = 0):
    super().__init__(flags)
    self.fd = fd
    self.path = path
    self.server = server

  def close(self):
    self.server._uos.close(self.fd)
  def read(self, offset, length):
    try:
      self.server._uos.lseek(self.fd, offset)
      return self.server._uos.read(self.fd, length)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    except:
      logger.error(traceback.format_exc())
      raise
  def write(self, offset, data):
    try:
      seek_ret = self.server._uos.lseek(self.fd, offset)
      logger.error(f"-> {seek_ret}")
      write_ret = self.server._uos.write(self.fd, data)
      logger.error(f"-> {write_ret}")
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    except:
      logger.error(traceback.format_exc())
      raise
    return paramiko.SFTP_OK

  def stat(self):
    try:
      return paramiko.SFTPAttributes.from_stat(
        self.server._uos.stat(self.path),
        pathname(self.path),
      )
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    except:
      logger.error(traceback.format_exc())
      raise
  
  def chattr(self, attr):
    return paramiko.SFTP_OK

class USFTPServer(paramiko.SFTPServerInterface):
  def __init__(self, server: USSHServer, *largs, **kwargs):
    super().__init__(server, *largs, **kwargs)
    self._server = server

  def list_folder(self, path):
    try:
      ret = [
        paramiko.SFTPAttributes.from_stat(
          self._server._uos.stat(path + '/' + fname),
          fname,
        )
        for fname in self._server._uos.listdir(path)
      ]
      logger.error(f"list_folder {ret}")
      return ret
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    except:
      logger.error(traceback.format_exc())
      raise

  def stat(self, path):
    try:
      return paramiko.SFTPAttributes.from_stat(
        self._server._uos.stat(path),
        pathname(path),
      )
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    except:
      logger.error(traceback.format_exc())
      raise
  lstat = stat

  def open(self, path, flags, attr):
    logger.error('USFTP Server open')
    try:
      binary_flag = getattr(os, 'O_BINARY',  0)
      flags |= binary_flag
      mode = getattr(attr, 'st_mode', None) or 0o666
      fd = self._server._uos.open(path, flags, mode)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    except:
      logger.error(traceback.format_exc())
      raise
    return USFTPHandle(fd, path, self._server, flags)

  def remove(self, path):
    try:
      self._server._uos.unlink(path)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    except:
      logger.error(traceback.format_exc())
      raise
    return paramiko.SFTP_OK

  def rename(self, oldpath, newpath):
    try:
      self._server._uos.rename(oldpath, newpath)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    except:
      logger.error(traceback.format_exc())
      raise
    return paramiko.SFTP_OK

  def mkdir(self, path, attr):
    try:
      self._server._uos.mkdir(path)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    except:
      logger.error(traceback.format_exc())
      raise
    return paramiko.SFTP_OK

  def rmdir(self, path):
    try:
      self._server._uos.rmdir(path)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    except:
      logger.error(traceback.format_exc())
      raise
    return paramiko.SFTP_OK

  def chattr(self, path, attr):
    return paramiko.SFTP_OK

  def symlink(self, target_path, path) -> int:
    return paramiko.SFTPServer.convert_errno(errno.ENOTSUP)

  def readlink(self, path) -> t.Union[str, int]:
    return paramiko.SFTPServer.convert_errno(errno.ENOENT)

def ufs_via_sftp(ufs: dict, host: str, port: int, username: str, password: t.Optional[str] = None, keyfile: t.Optional[str] = None, BACKLOG = 10):
  import socket
  if keyfile is None: keyfile = str(pathlib.Path('~/.ssh/id_rsa').expanduser())
  with SyncUFS.from_dict(**ufs) as ufs_:
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    server_socket.bind((host, port))
    server_socket.listen(BACKLOG)
    server = USSHServer(ufs_, username, password)
    connections = []

    while True:
      conn, addr = server_socket.accept()
      host_key = paramiko.RSAKey.from_private_key_file(keyfile)
      transport = paramiko.Transport(conn)
      transport.add_server_key(host_key)
      transport.set_subsystem_handler(
        'sftp', paramiko.SFTPServer, USFTPServer
      )
      try:
        transport.start_server(server=server)
        channel = transport.accept()
        connections.append((conn, addr, transport, channel))
      except KeyboardInterrupt:
        raise
      except:
        logger.error(traceback.print_exc())
        continue

@contextlib.contextmanager
def serve_ufs_via_sftp(ufs: SyncUFS, host: str, port: int, username: str, password: t.Optional[str] = None, keyfile: t.Optional[str] = None, BACKLOG = 10):
  import multiprocessing as mp
  from ufs.utils.process import active_process
  from ufs.utils.polling import wait_for
  from ufs.utils.socket import nc_z
  mp_spawn = mp.get_context('spawn')
  with active_process(mp_spawn.Process(
    target=ufs_via_sftp,
    kwargs=dict(
      ufs=ufs.to_dict(),
      host=host,
      port=port,
      username=username,
      password=password,
      keyfile=keyfile,
      BACKLOG=BACKLOG,
    ),
  )):
    wait_for(lambda: nc_z(host, port))
    yield

if __name__ == '__main__':
  import sys, json
  kwargs = json.loads(sys.argv[1])
  ufs_via_sftp(**kwargs)
