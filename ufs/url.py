import functools
import contextlib
from ufs.utils.url import parse_url, parse_qs
from ufs.utils.pathlib import SafePurePosixPath

protos = {}
def register_proto_handler(proto):
  def decorator(func):
    protos[proto] = func
    return func
  return decorator

@functools.lru_cache()
def ufs_local():
  from ufs.impl.local import Local
  return Local()

@register_proto_handler(None)
@register_proto_handler('file')
def proto_file(url):
  from ufs.impl.prefix import Prefix
  return Prefix(ufs_local(), url['path'])

@functools.lru_cache()
def ufs_memory():
  from ufs.impl.memory import Memory
  return Memory()

@register_proto_handler('memory')
def proto_memory(url):
  from ufs.impl.prefix import Prefix
  return Prefix(ufs_memory(), url['path'])

@functools.lru_cache()
def ufs_rclone(**kwargs):
  from ufs.impl.rclone import RClone
  return RClone(**kwargs)

@register_proto_handler('rclone')
def proto_rclone(url):
  from ufs.impl.prefix import Prefix
  return Prefix(ufs_rclone(**parse_qs(url)), url['path'])

@functools.lru_cache()
def ufs_s3(**kwargs):
  from ufs.impl.s3 import S3
  return S3(**kwargs)

@register_proto_handler('s3')
def proto_s3(url):
  from ufs.impl.prefix import Prefix
  return Prefix(ufs_s3(**parse_qs(url)), url['path'])

@functools.lru_cache()
def ufs_sbfs(**kwargs):
  from ufs.impl.sbfs import SBFS
  from ufs.impl.sync import Sync
  return Sync(SBFS(**kwargs))

@register_proto_handler('sbfs')
def proto_sbfs(url):
  from ufs.impl.prefix import Prefix
  return Prefix(ufs_sbfs(**parse_qs(url)), url['path'])

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
def open_from_url(url: str, mode='r', protos=protos):
  '''
  Usage:
  with open_from_url('s3://mybucket/myprefix/my_file.tsv?anon=true') as fr:
    print(fr.read())
  '''
  url_parsed = parse_url(url)
  if url_parsed['proto'] not in protos:
    raise NotImplementedError(url_parsed['proto'])
  path = SafePurePosixPath(url_parsed['path'])
  ufs = protos[url_parsed['proto']](dict(url_parsed, path=path.parent))
  with ufs:
    from ufs.pathlib import UPath
    upath = UPath(ufs)
    with (upath/path.name).open(mode) as fr:
      yield fr
