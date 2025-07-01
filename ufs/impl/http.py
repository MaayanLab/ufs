''' UFS interface for accessing HTTP
'''
import requests
from ufs.spec import DescriptorFromAtomicMixin, SyncUFS, AccessScope

class HTTP(DescriptorFromAtomicMixin, SyncUFS):
  def __init__(self, netloc: str, scheme='https', headers={}) -> None:
    super().__init__()
    self._scheme = scheme
    self._netloc = netloc
    self._headers = headers

  def scope(self):
    return AccessScope.universe

  @staticmethod
  def from_dict(*, netloc, scheme, headers):
    return HTTP(
      netloc=netloc,
      scheme=scheme,
      headers=headers,
    )

  def to_dict(self):
    return dict(SyncUFS.to_dict(self),
      netloc=self._netloc,
      scheme=self._scheme,
      headers=self._headers,
    )

  def info(self, path):
    req = requests.head(self._scheme + '://' + self._netloc + str(path), headers=self._headers, allow_redirects=True)
    if req.status_code == 404:
      raise FileNotFoundError(path)
    elif req.status_code in {401, 403}:
      raise PermissionError(path)
    elif req.status_code > 299:
      raise RuntimeError(req.status_code)
    return {
      'type': 'file',
      'size': int(req.headers.get('Content-Length', 0)),
    }

  def cat(self, path):
    req = requests.get(self._scheme + '://' + self._netloc + str(path), headers=self._headers, allow_redirects=True)
    if req.status_code == 404:
      raise FileNotFoundError(path)
    elif req.status_code in {401, 403}:
      raise PermissionError(path)
    elif req.status_code > 299:
      raise RuntimeError(req.status_code)
    yield from req.iter_content(self.CHUNK_SIZE)
