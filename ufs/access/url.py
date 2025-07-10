''' url-style access for UFS, similar to fsspec's urls but all backed by UFS
'''
import contextlib
from ufs.utils.url import parse_url, parse_netloc, parse_fragment_qs
from ufs.utils.pathlib import SafePurePosixPath

protos = {}
def register_proto_handler(proto):
  def decorator(func):
    protos[proto] = func
    return func
  return decorator

@register_proto_handler(None)
@register_proto_handler('file')
def proto_file(url):
  from ufs.impl.local import Local
  from ufs.impl.prefix import Prefix
  return Prefix(Local(), url['path'])

@register_proto_handler('memory')
def proto_memory(url):
  from ufs.impl.memory import Memory
  from ufs.impl.prefix import Prefix
  return Prefix(Memory(), url['path'])

@register_proto_handler('tmp')
def proto_tmp(url):
  from ufs.impl.tempdir import TemporaryDirectory
  return TemporaryDirectory()

@register_proto_handler('rclone')
def proto_rclone(url):
  from ufs.impl.rclone import RClone
  from ufs.impl.prefix import Prefix
  return Prefix(RClone(**parse_fragment_qs(url)), url['path'])

@register_proto_handler('s3')
def proto_s3(url):
  from ufs.impl.s3fs import S3
  from ufs.impl.prefix import Prefix
  return Prefix(S3(**parse_fragment_qs(url)), url['path'])

@register_proto_handler('sbfs')
def proto_sbfs(url):
  from ufs.impl.prefix import Prefix
  from ufs.impl.sbfs import SBFS
  from ufs.impl.sync import Sync
  return Prefix(Sync(SBFS(**parse_fragment_qs(url))), url['path'])

@register_proto_handler('ftps')
@register_proto_handler('ftp')
def proto_ftp(url):
  from ufs.impl.ftp import FTP
  from ufs.impl.prefix import Prefix
  netloc_parsed = parse_netloc(url)
  return Prefix(
    FTP(
      host=netloc_parsed['host'],
      user=netloc_parsed.get('username') or '',
      passwd=netloc_parsed.get('password') or '',
      port=netloc_parsed.get('port') or 21,
      tls=url['proto'] == 'ftps',
      **parse_fragment_qs(url)
    ),
    netloc_parsed['path']
  )

@register_proto_handler('https')
@register_proto_handler('http')
def proto_http(url):
  from ufs.impl.http import HTTP
  from ufs.impl.prefix import Prefix
  netloc_parsed = parse_netloc(url)
  return Prefix(
    HTTP(
      netloc_parsed['netloc'],
      scheme=url['proto'],
      **parse_fragment_qs(url)
    ),
    netloc_parsed['path']
  )

@register_proto_handler('sftp')
def proto_sftp(url):
  from ufs.impl.sftp import SFTP
  from ufs.impl.prefix import Prefix
  netloc_parsed = parse_netloc(url)
  return Prefix(
    SFTP(
      host=netloc_parsed['host'],
      port=netloc_parsed.get('port') or 22,
      username=netloc_parsed.get('username'),
      password=netloc_parsed.get('password'),
      **parse_fragment_qs(url)
    ),
    netloc_parsed['path']
  )

@register_proto_handler('drs')
def proto_drs(url):
  from ufs.impl.drs import DRS
  from ufs.impl.prefix import Prefix
  netloc_parsed = parse_netloc(url)
  opts = dict(scheme='http' if netloc_parsed['port'] else 'https')
  opts.update(parse_fragment_qs(url))
  return Prefix(
    DRS(**opts),
    url['path']
  )

def ufs_from_url(url: str, protos=protos):
  '''
  Usage:
  ufs = ufs_from_url('s3://mybucket/myprefix/')
  '''
  url_parsed = parse_url(url)
  if url_parsed['proto'] not in protos:
    raise NotImplementedError(url_parsed['proto'])
  return protos[url_parsed['proto']](url_parsed)

def ufs_file_from_url(url: str, filename=None, protos=protos):
  '''
  The resulting ufs is a one-file-system containing only the filename of the url
  Usage:
  ufs, filename = ufs_from_url('s3://mybucket/myprefix/my_file.tsv#?anon=true')

  :param url: The url to be turned into a ufs
  :param filename: The filename to use for the file, defaults to the final name component of the path
  :returns: ufs, filename
  '''
  from ufs.impl.mapper import Mapper
  url_parsed = parse_url(url)
  if url_parsed['proto'] not in protos:
    raise NotImplementedError(url_parsed['proto'])
  ufs = protos[url_parsed['proto']](url_parsed)
  if filename is None:
    path = SafePurePosixPath(url_parsed['path'])
    filename = path.name
  return Mapper({ filename: ufs }), filename

@contextlib.contextmanager
def upath_from_url(url: str, protos=protos):
  '''
  Usage:
  ufs = ufs_from_url('s3://mybucket/myprefix/')
  '''
  from ufs.access.pathlib import UPath
  with ufs_from_url(url, protos=protos) as ufs:
    yield UPath(ufs)

@contextlib.contextmanager
def open_from_url(url: str, mode='r', protos=protos):
  '''
  Usage:
  with open_from_url('s3://mybucket/myprefix/my_file.tsv#?anon=true') as fr:
    print(fr.read())
  '''
  with upath_from_url(url, protos=protos) as upath:
    with upath.open(mode) as fh:
      yield fh
