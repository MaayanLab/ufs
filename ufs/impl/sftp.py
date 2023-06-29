''' SFTP Client as a UFS
'''
import stat
import paramiko
import itertools

from ufs.spec import UFS

class SFTP(UFS):
  def __init__(self, host: str, username: str = None, password: str = None, port: int = 22):
    super().__init__()
    self._host = host
    self._port = port
    self._username = username
    self._password = password
    self._cfd = iter(itertools.count(start=5))
    self._fds = {}

  @staticmethod
  def from_dict(*, host, username, password, port):
    return SFTP(
      host=host,
      username=username,
      password=password,
      port=port,
    )

  def to_dict(self):
    return dict(super().to_dict(),
      host=self._host,
      username=self._username,
      password=self._password,
      port=self._port,
    )

  def start(self):
    self._ssh = paramiko.SSHClient()
    self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    self._ssh.connect(self._host, username=self._username, password=self._password, port=self._port)
    self._sftp = self._ssh.open_sftp()

  def stop(self):
    self._sftp.close()
    self._ssh.close()

  def ls(self, path):
    return self._sftp.listdir(str(path))

  def info(self, path):
    info = self._sftp.stat(str(path))
    if info.st_mode & stat.S_IFDIR:
      type = 'directory'
    elif info.st_mode & stat.S_IFREG:
      type = 'file'
    else:
      raise NotImplementedError()
    return {
      'type': type,
      'size': info.st_size,
      'atime': info.st_atime,
      'mtime': info.st_mtime,
    }

  def open(self, path, mode, *, size_hint = None):
    fd = next(self._cfd)
    self._fds[fd] = self._sftp.open(str(path), mode)
    return fd

  def seek(self, fd, pos, whence = 0):
    return self._fds[fd].seek(pos, whence)
  def read(self, fd, amnt):
    return self._fds[fd].read(amnt)
  def write(self, fd, data: bytes):
    self._fds[fd].write(data)
    return len(data)
  def truncate(self, fd, length):
    self._fds[fd].truncate(length)
  def flush(self, fd):
    self._fds[fd].flush()
  def close(self, fd):
    return self._fds.pop(fd).close()

  def unlink(self, path):
    self._sftp.remove(str(path))

  def mkdir(self, path):
    try:
      self._sftp.stat(str(path))
    except FileNotFoundError:
      self._sftp.mkdir(str(path))
    else:
      raise FileExistsError(str(path))

  def rmdir(self, path):
    self._sftp.rmdir(str(path))

  def copy(self, src, dst):
    stdin, stdout, stderr = self._ssh.exec_command('cp $UFS_COPY_SRC $UFS_COPY_DST', environment=dict(UFS_COPY_SRC=str(src), UFS_COPY_DST=str(dst)))
    if stdout.channel.recv_exit_status() != 0:
      raise RuntimeError(f"cp resulted in {stderr.read()}")

  def rename(self, src, dst):
    self._sftp.rename(str(src), str(dst))
