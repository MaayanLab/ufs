import re
import json
import typing as t
from ufs.utils.one import one

class URLParsed(t.TypedDict):
  proto: t.Optional[str]
  path: str
  qs: t.Optional[str]
  fragment: t.Optional[str]

url_expr = re.compile(r'((?P<proto>[^:]+)://)?(?P<path>[^\?#]*)(\?(?P<qs>[^#]*))?(#(?P<fragment>.*))?')

def parse_url(url) -> URLParsed:
  m = url_expr.match(url)
  if not m: raise RuntimeError(f"Invalid url: {url}")
  return m.groupdict()

def try_json_loads(s):
  try: return json.loads(s)
  except: return s

def parse_qs(url_parsed: URLParsed):
  from urllib.parse import parse_qs
  return { k: try_json_loads(one(v)) for k, v in parse_qs(url_parsed['qs'] or '').items() }
