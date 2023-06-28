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

@register_proto_handler('rclone')
def proto_rclone(url):
  from ufs.impl.rclone import RClone
  from ufs.impl.prefix import Prefix
  return Prefix(RClone(**parse_fragment_qs(url)), url['path'])

@register_proto_handler('s3')
def proto_s3(url):
  from ufs.impl.s3 import S3
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
      netloc_parsed['host'],
      user=netloc_parsed.get('username'),
      passwd=netloc_parsed.get('password'),
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

@register_proto_handler('drs')
def proto_drs(url):
  from ufs.impl.drs import DRS
  from ufs.impl.prefix import Prefix
  return Prefix(
    DRS(),
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
  url_parsed = parse_url(url)
  if url_parsed['proto'] not in protos:
    raise NotImplementedError(url_parsed['proto'])
  path = SafePurePosixPath(url_parsed['path'])
  ufs = protos[url_parsed['proto']](dict(url_parsed, path=path.parent))
  with ufs:
    from ufs.access.pathlib import UPath
    upath = UPath(ufs)
    with (upath/path.name).open(mode) as fr:
      yield fr