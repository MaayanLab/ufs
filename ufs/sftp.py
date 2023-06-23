import os
import errno
import paramiko, paramiko.common
from ufs.os import UOS
from ufs.spec import UFS

class USSHServer(paramiko.ServerInterface):
  def __init__(self, ufs: UFS, username: str, password: str = None):
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
    if username == self.username and self._password is None:
      return paramiko.common.AUTH_SUCCESSFUL
    else:
      return paramiko.common.AUTH_FAILED

  def check_auth_password(self, username, password):
    if username == self._username and password == self._password:
      return paramiko.common.AUTH_SUCCESSFUL
    else:
      return paramiko.common.AUTH_FAILED

class USFTPHandle(paramiko.SFTPHandle):
  def __init__(self, fd: int, filename: str, server: 'USFTPServer', flags: int = 0):
    super().__init__(flags)
    self.filename = filename
    self._fd = fd
    self._server = server

  def stat(self):
    try:
      return paramiko.SFTPAttributes.from_stat(self._server._uos.stat(self.filename))
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
  
  def read(self, offset: int, length: int):
    try:
      self._server._uos.seek(self._fd, offset)
      return self._server._uos.read(self._fd, length)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)

  def write(self, offset, data):
    try:
      self._server._uos.seek(self._fd, offset)
      return self._server._uos.write(self._fd, data)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)

  def chattr(self, attr):
    return paramiko.SFTP_OK

  def close(self):
    self._server._uos.close(self._fd)

class USFTPServer(paramiko.SFTPServerInterface):
  def __init__(self, server: USSHServer, *largs, **kwargs):
    super().__init__(server, *largs, **kwargs)
    self._server = server

  def list_folder(self, path):
    try:
      return [
        paramiko.SFTPAttributes.from_stat(self._server._uos.stat(path + '/' + fname))
        for fname in self._server._uos.listdir(path)
      ]
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)

  def stat(self, path):
    try:
      return paramiko.SFTPAttributes.from_stat(self._server._uos.stat(path))
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
  lstat = stat

  def open(self, path, flags, attr):
    try:
      binary_flag = getattr(os, 'O_BINARY',  0)
      flags |= binary_flag
      mode = getattr(attr, 'st_mode', None) or 0o666
      fd = self._server._uos.open(path, flags, mode)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    return USFTPHandle(fd, path, self._server, flags)

  def remove(self, path):
    try:
      self._server._uos.unlink(path)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    return paramiko.SFTP_OK

  def rename(self, oldpath, newpath):
    try:
      self._server._uos.rename(oldpath, newpath)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    return paramiko.SFTP_OK

  def mkdir(self, path, attr):
    try:
      self._server._uos.mkdir(path)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    return paramiko.SFTP_OK

  def rmdir(self, path):
    try:
      self._server._uos.rmdir(path)
    except OSError as e:
      return paramiko.SFTPServer.convert_errno(e.errno)
    return paramiko.SFTP_OK

  def chattr(self, path, attr):
    return paramiko.SFTP_OK

  def symlink(self, target_path, path) -> int:
    return paramiko.SFTPServer.convert_errno(errno.ENOTSUP)

  def readlink(self, path) -> str | int:
    return paramiko.SFTPServer.convert_errno(errno.ENOENT)

def serve_ufs_via_sftp(ufs: UFS, host: str, port: int, keyfile: str, username: str, password: str = None, BACKLOG = 10):
  import time, socket
  server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
  server_socket.bind((host, port))
  server_socket.listen(BACKLOG)

  while True:
    conn, addr = server_socket.accept()
    host_key = paramiko.RSAKey.from_private_key_file(keyfile)
    transport = paramiko.Transport(conn)
    transport.add_server_key(host_key)
    transport.set_subsystem_handler(
      'sftp', paramiko.SFTPServer, USFTPServer
    )
    server = USSHServer(ufs, username, password)
    transport.start_server(server=server)

    channel = transport.accept()
    while transport.is_active():
      time.sleep(1)
