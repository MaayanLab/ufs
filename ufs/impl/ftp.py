import queue
import ftplib
import itertools
import contextlib
import threading as t
from ufs.spec import DescriptorFromAtomicMixin, UFS, QueuedIterator, ReadableIterator
from ufs.utils.one import one

# unlike ftplib.FTP_TLS, this version connects over SSL first and then runs FTP over it
#  ftplib.FTP_TLS is more like email's STARTTLS in that it first connects, then upgrades to ssl
class ftplib_FTPS(ftplib.FTP):
  def __init__(self):
    super().__init__()
  
  def connect(self, hostname, port=990):
    import ssl, socket
    context = ssl.create_default_context()
    sock = socket.create_connection((hostname, port))
    self.sock = context.wrap_socket(sock, server_hostname=hostname)
    self.af = self.sock.family
    self.file = self.sock.makefile('r')
    self.welcome = self.getresp()
    return self.welcome

def ftp_client_thread(send: queue.Queue, recv: queue.Queue, opts: dict):
  if opts['starttls']:
    ftp = ftplib.FTP_TLS()
  elif opts['tls']:
    ftp = ftplib_FTPS()
  else:
    ftp = ftplib.FTP()
  ftp.connect(opts['host'], opts['port'])
  ftp.login(opts['user'], opts['passwd'])
  if isinstance(ftp, ftplib.FTP_TLS): ftp.prot_p()
  while True:
    msg = recv.get()
    i, op, args, kwargs = msg
    if op == None:
      recv.task_done()
      break
    try:
      func = getattr(ftp, op)
      if op in {'retrlines', 'retrbinary'}:
        cmd, stream = args
        res = func(cmd, stream.write, **kwargs)
      elif op in {'storbinary'}:
        cmd, stream = args
        res = func(cmd, ReadableIterator(iter(stream.get, None)), **kwargs)
      else:
        res = func(*args, **kwargs)
    except Exception as err:
      send.put([i, None, err])
    else:
      send.put([i, res, None])
    finally:
      if op in {'retrlines', 'retrbinary'}: stream.close()
    recv.task_done()
  ftp.quit()

class FTP(DescriptorFromAtomicMixin, UFS):
  def __init__(self, host: str, user = '', passwd = '', port = 21, tls = False, starttls = None) -> None:
    super().__init__()
    self._host = host
    self._user = user
    self._passwd = passwd
    self._port = port
    self._tls = tls
    self._starttls = True if port == 21 and tls and starttls is None else bool(starttls)
    self._taskid = iter(itertools.count())

  @staticmethod
  def from_dict(*, host, user, passwd, port, tls, starttls):
    return FTP(
      host=host,
      user=user,
      passwd=passwd,
      port=port,
      tls=tls,
      starttls=starttls,
    )

  def to_dict(self):
    return dict(UFS.to_dict(self),
      host=self._host,
      user=self._user,
      passwd=self._passwd,
      port=self._port,
      tls=self._tls,
      starttls=self._starttls,
    )

  @contextlib.contextmanager
  def _translate_exc(self):
    try:
      yield
    except ftplib.error_perm as e:
      if 'No such file or directory.' in str(e):
        raise FileNotFoundError(e)
      elif 'File exists.' in str(e):
        raise FileExistsError(e)
      else:
        raise e

  def _forward(self, op, *args, **kwargs):
    self.start()
    i, i_ = next(self._taskid), None
    self._send.put([i, op, args, kwargs])
    while True:
      i_, ret, err = self._recv.get()
      self._recv.task_done()
      if i == i_:
        break
      # a different result came before ours, add it back to the queue and try again
      self._recv.put([i_, ret, err])
    if err is not None:
      with self._translate_exc():
        raise err
    else: return ret

  @contextlib.contextmanager
  def _forward_queue(self, op, *args, **kwargs):
    self.start()
    i, i_ = next(self._taskid), None
    self._send.put([i, op, args, kwargs])
    yield
    while True:
      i_, ret, err = self._recv.get()
      self._recv.task_done()
      if i == i_:
        break
      # a different result came before ours, add it back to the queue and try again
      self._recv.put([i_, ret, err])
    if err is not None:
      with self._translate_exc():
        raise err

  def start(self):
    if not hasattr(self, '_ftp_client_thread'):
      self._send, self._recv = queue.Queue(), queue.Queue()
      self._ftp_client_thread = t.Thread(
        target=ftp_client_thread,
        args=(self._recv, self._send, self.to_dict()),
      )
      self._ftp_client_thread.start()
  
  def stop(self):
    if hasattr(self, '_ftp_client_thread'):
      self._send.put([next(iter(self._taskid)), None, None, None])
      self._ftp_client_thread.join()
      del self._ftp_client_thread

  def ls(self, path):
    path_str = str(path)
    nlst = self._forward('nlst', path_str)
    # it seems nlst sometimes returns the entire path not just the relative path
    files = []
    for f in nlst:
      if f.startswith(path_str):
        files.append(f[len(path_str):])
      else:
        files.append(f)
    return files

  def info(self, path):
    if str(path) == '/': return { 'type': 'directory', 'size': 0 }
    stream = QueuedIterator()
    with self._forward_queue('retrlines', 'LIST ' + str(path.parent), stream):
      try:
        info = one(
          {
            # the first character (part of the permissions bits) will be '-' for a file and 'd' for a directory
            'type': line[0],
            # the 5th field contains the size
            'size': int(line_split[4]),
            # right after that is the date until the filename
            'time': ' '.join(line_split[5:7]),
          }
          for line in stream
          for line_split in (line.split(maxsplit=9),)
          # the last field will be the filename
          if line_split[-1] == path.name
        )
      except StopIteration:
        raise FileNotFoundError(path)
    if info['type'] == 'd':
      return { 'type': 'directory', 'size': 0 }
    else:
      return { 'type': 'file', 'size': info['size'] }

  def cat(self, path):
    stream = QueuedIterator()
    with self._forward_queue('retrbinary', 'RETR ' + str(path), stream):
      yield from stream

  def put(self, path, data, *, size_hint = None):
    stream = queue.Queue()
    try:
      with self._forward_queue('storbinary', 'STOR ' + str(path), stream):
        for chunk in data:
          stream.put(chunk)
        stream.put(None)
    except ftplib.error_perm as e:
      raise PermissionError(str(path)) from e

  def unlink(self, path):
    self._forward('delete', str(path))

  def mkdir(self, path):
    self._forward('mkd', str(path))

  def rmdir(self, path):
    self._forward('rmd', str(path))

  def rename(self, src, dst):
    self._forward('rename', str(src), str(dst))
