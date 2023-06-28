import stat
import paramiko
import itertools

from ufs.spec import UFS

class SFTP(UFS):
  def __init__(self, host: str, port: int, username: str = None, password: str = None):
    super().__init__()
    self._host = host
    self._port = port
    self._username = username
    self._password = password
    self._cfd = iter(itertools.count(start=5))
    self._fds = {}

  @staticmethod
  def from_dict(*, host, port, username, password):
    return SFTP(
      host=host,
      port=port,
      username=username,
      password=password,
    )

  def to_dict(self):
    return dict(super().to_dict(),
      host=self._host,
      port=self._port,
      username=self._username,
      password=self._password,
    )

  def start(self):
    self._ssh = paramiko.SSHClient()
    self._ssh.connect(self._host, self._port, username=self._username, password=self._password)
    self._sftp = self._ssh.open_sftp()

  def stop(self):
    self._sftp.close()
    self._transport.close()

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
    return self._fds[fd].write(data)
  def truncate(self, fd, length):
    self._fds[fd].truncate(length)
  def flush(self, fd):
    self._fds[fd].flush()
  def close(self, fd):
    return self._fds.pop(fd).close()

  def unlink(self, path):
    self._sftp.remove(str(path))

  def mkdir(self, path):
    self._sftp.mkdir(str(path))

  def rmdir(self, path):
    self._sftp.rmdir(str(path))

  def copy(self, src, dst):
    stdin, stdout, stderr = self._ssh.exec_command('cp $UFS_COPY_SRC $UFS_COPY_DST', environment=dict(UFS_COPY_SRC=str(src), UFS_COPY_DST=str(dst)))
    if stdout.channel.recv_exit_status() != 0:
      raise RuntimeError(f"cp resulted in {stderr.read()}")

  def rename(self, src, dst):
    self._sftp.rename(src, dst)
