import ftplib
import threading as t
from ufs.spec import DescriptorFromAtomicMixin, UFS, QueuedIterator, ReadableIterator
from ufs.utils.one import one

class FTP(DescriptorFromAtomicMixin, UFS):
  def __init__(self, host: str, user = '', passwd = '', tls = False) -> None:
    super().__init__()
    self._host = host
    self._user = user
    self._passwd = passwd
    self._tls = tls

  @staticmethod
  def from_dict(*, host, user, passwd, tls):
    return FTP(
      host=host,
      user=user,
      passwd=passwd,
      tls=tls,
    )

  def to_dict(self):
    return dict(UFS.to_dict(self),
      host=self._host,
      user=self._user,
      passwd=self._passwd,
      tls=self._tls,
    )

  def start(self):
    if not hasattr(self, '_ftp'):
      self._ftp = ftplib.FTP_TLS(self._host) if self._tls else ftplib.FTP(self._host)
      self._ftp.login(self._user, self._passwd)
  
  def stop(self):
    if hasattr(self, '_ftp'):
      self._ftp.quit()
      del self._ftp

  def ls(self, path):
    return self._ftp.nlst(str(path))
  
  def info(self, path):
    facts = one(
      facts
      for file, facts in self._ftp.mlsd(str(path.parent), ['type', 'size'])
      if file == path.name
    )
    if facts['type'] == 'dir':
      return { 'type': 'directory', 'size': 0 }
    else:
      return { 'type': 'file', 'size': facts['size'] }

  def cat(self, path):
    queued_iterator = QueuedIterator()
    thread = t.Thread(target=self._ftp.retrbinary, args=('RETR ' + str(path), queued_iterator.write,))
    thread.start()
    yield from queued_iterator
    thread.join()

  def put(self, path, data, *, size_hint = None):
    self._ftp.storbinary('STOR ' + str(path), ReadableIterator(data))

  def unlink(self, path):
    self._ftp.delete(str(path))

  def mkdir(self, path):
    self._ftp.mkd(str(path))

  def rmdir(self, path):
    self._ftp.rmd(str(path))

  def rename(self, src, dst):
    self._ftp.rename(str(src), str(dst))
