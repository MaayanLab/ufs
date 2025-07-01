''' A GA4GH-compatible DRS client in the form of a UFS store.

Maps paths of like /hostname/opaque_id/bundle_content_by_name => drs://hostname/opaque_id
'''
import requests
from ufs.spec import DescriptorFromAtomicMixin, SyncUFS, AccessScope
from ufs.utils.one import one
from ufs.utils.pathlib import SafePurePosixPath

class DRS(DescriptorFromAtomicMixin, SyncUFS):
  def __init__(self, scheme='https', headers={}):
    super().__init__()
    self._scheme = scheme
    self._headers = headers

  def scope(self):
    return AccessScope.universe

  @staticmethod
  def from_dict(*, scheme, headers):
    return DRS(
      scheme=scheme,
      headers=headers,
    )

  def to_dict(self):
    return dict(super().to_dict(),
      scheme=self._scheme,
      headers=self._headers,
    )

  def _info(self, host, opaque_id, expand=False):
    ''' DRS Object Info
    '''
    url = self._scheme + '://' + host + '/ga4gh/drs/v1/objects/' + opaque_id + ('?expand=true' if expand else '')
    req = requests.get(url, headers=self._headers)
    if req.status_code == 404:
      raise FileNotFoundError('/' + host + '/' + opaque_id)
    elif req.status_code in {401, 403}:
      raise PermissionError('/' + host + '/' + opaque_id)
    elif req.status_code > 299:
      raise RuntimeError(req.status_code)
    return req.json()

  def _flatten(self, path):
    ''' Any sub-object of a DRS bundle, has a unique DRS id, flatten gets the leaf path. This trick
    lets us treat bundles as subpaths but we only ever query /host/opaque_id
    '''
    _, host, opaque_id, *subpath = path.parts
    if not subpath:
      return path, self._info(host, opaque_id, expand=False)
    else:
      info = self._info(host, opaque_id, expand=True)
      for i in range(len(subpath)):
        try:
          info = one(item for item in info.get('contents', []) if item['name'] == subpath[i])
        except StopIteration:
          raise NotADirectoryError(SafePurePosixPath()/host/opaque_id/'/'.join(subpath[:i]))
      return SafePurePosixPath()/host/info['id'], info

  def info(self, path):
    flat_path, info = self._flatten(path)
    _, host, opaque_id = flat_path.parts
    if info.get('contents') is not None:
      return { 'type': 'directory', 'size': 0 }
    elif info.get('size') is None:
      info = self._info(host, opaque_id, expand=False)
    return { 'type': 'file', 'size': info['size'], 'drs': f"drs://{host}/{opaque_id}" }

  def ls(self, path):
    _flat_path, info = self._flatten(path)
    if info.get('contents') is None:
      raise NotADirectoryError(path)
    return [item['name'] for item in info['contents']]

  def cat(self, path):
    flat_path, info = self._flatten(path)
    if info.get('contents') is not None:
      raise IsADirectoryError(path)
    elif info.get('access_methods') is None:
      _, host, opaque_id = flat_path.parts
      info = self._info(host, opaque_id, expand=False)
    # find an acceptable access method
    for access_method in info.get('access_methods', []):
      # obtain access url
      if access_method.get('access_url'):
        access_url = access_method['access_url']
      elif access_method.get('access_id'):
        _, host, opaque_id = flat_path.parts
        req = requests.get(self._scheme + '://' + host + '/ga4gh/drs/v1/objects/' + opaque_id + '/access/' + access_method['access_id'], headers=self._headers)
        if req.status_code == 404:
          raise FileNotFoundError(path)
        elif req.status_code in {401, 403}:
          raise PermissionError(path)
        elif req.status_code > 299:
          raise RuntimeError(req.status_code)
        access_url = req.json()
      assert type(access_url) == dict and access_url.get('url'), 'DRS Access URL should be a dictionary'
      import json
      from urllib.parse import quote
      if access_url.get('headers'):
        access_url['url'] += f"#?headers={quote(json.dumps(dict(self._headers, **access_url['headers'])))}"
      elif self._headers:
        access_url['url'] += f"#?headers={quote(json.dumps(self._headers))}"
      # simply attempt to fetch from the access_url using ufs_from_url
      #  this supports various providers include http, ftp, and s3
      from ufs.access.url import ufs_from_url
      try:
        with ufs_from_url(access_url['url']) as ufs:
          yield from ufs.cat('/')
        return
      except:
        pass
    raise RuntimeError(f"Failed to fetch object from any of the {info.get('access_methods')}")
