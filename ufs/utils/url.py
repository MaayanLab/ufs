import re
import json
import typing as t
from ufs.utils.one import one

TypedDict = t.TypedDict if getattr(t, 'TypedDict', None) else dict

class URLParsed(TypedDict):
  proto: t.Optional[str]
  path: str
  fragment: t.Optional[str]

url_expr = re.compile(r'((?P<proto>[^:]+)://)?(?P<path>[^#]*)(#(?P<fragment>.*))?')

def parse_url(url) -> URLParsed:
  ''' We parse the url, separating protocol, path, fragment and a query string in the fragment
   (proto:///some/path#fragment_section)
  We ignore the regular query string since it could be used by `http` for instance. Fragments however
   are invalid as server-side urls since they are browser side only, thus we will use qs notation in
   the fragment section of the url for options overrides.
  '''
  m = url_expr.match(url)
  if not m: raise RuntimeError(f"Invalid url: {url}")
  return m.groupdict()


class NetlocParsed(TypedDict):
  username: t.Optional[str]
  password: t.Optional[str]
  host: t.Optional[str]
  port: t.Optional[int]
  netloc: str
  path: str

netloc_expr = re.compile(r'^((?P<username>[^:@]+)(:(?P<password>[^@]+))?@)?(?P<host>[^@:]+)(:(?P<port>\d+))?$')

def parse_netloc(url_parsed: URLParsed) -> NetlocParsed:
  netloc, sep, path = url_parsed['path'].partition('/')
  m = netloc_expr.match(netloc)
  if not m: raise RuntimeError(f"Invalid netloc: {netloc}")
  netloc_parsed = m.groupdict()
  return dict(
    netloc_parsed,
    netloc=netloc,
    port=int(netloc_parsed['port']) if netloc_parsed.get('port') else None,
    path=(sep or '') + (path or ''),
  )


def try_json_loads(s):
  try: return json.loads(s)
  except: return s

def parse_fragment_qs(url_parsed: URLParsed):
  from urllib.parse import parse_qs
  _, _, qs = (url_parsed.get('fragment') or '').partition('?')
  return { k: try_json_loads(one(v)) for k, v in parse_qs(qs or '').items() }
