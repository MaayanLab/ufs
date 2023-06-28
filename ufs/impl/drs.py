import requests
from ufs.spec import DescriptorFromAtomicMixin, UFS
from ufs.utils.one import one
from ufs.utils.pathlib import SafePurePosixPath

class DRS(DescriptorFromAtomicMixin, UFS):
  def _info(self, host, opaque_id, expand=False):
    ''' DRS Object Info
    '''
    url = 'https://' + host + '/ga4gh/drs/v1/objects/' + opaque_id + ('?expand=true' if expand else '')
    req = requests.get(url)
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
        if info.get('contents') is None:
          raise NotADirectoryError(SafePurePosixPath('/')/host/opaque_id/'/'.join(subpath[:i]))
        info = one(item['name'] for item in info['contents'] if item['name'] == subpath[i])
      return SafePurePosixPath('/')/host/info['id'], info

  def info(self, path):
    flat_path, info = self._flatten(path)
    if info.get('contents') is not None:
      return { 'type': 'directory', 'size': 0 }
    elif info.get('size') is None:
      _, host, opaque_id = flat_path.parts
      info = self._info(host, opaque_id, expand=False)
    return { 'type': 'file', 'size': info['size'] }

  def ls(self, path):
    _flat_path, info = self._flatten(path)
    if info.get('contents') is None:
      raise NotADirectoryError(path)
    return [item['name'] for item in info['contents']]

  def cat(self, path):
    flat_path, info = self._flatten(path).parts
    if info.get('contents') is not None:
      raise IsADirectoryError(path)
    elif info.get('access_methods') is None:
      _, host, opaque_id = flat_path.parts
      info = self._info(host, opaque_id, expand=False)
    # find an acceptable access method
    for access_method in info.get('access_methods', []):
      # obtain access url
      if access_method.get('access_url'):
        access_url = dict(url=access_method['access_url'])
      elif access_method.get('access_id'):
        _, host, opaque_id = flat_path.parts
        req = requests.get('https://' + host + '/ga4gh/drs/v1/objects/' + opaque_id + '/access/', access_method['access_id'])
        if req.status_code == 404:
          raise FileNotFoundError(path)
        elif req.status_code in {401, 403}:
          raise PermissionError(path)
        elif req.status_code > 299:
          raise RuntimeError(req.status_code)
        access_url = req.json()
        if access_url.get('headers'):
          import json
          from urllib.parse import quote
          access_url['url'] += f"#?headers={quote(json.dumps(access_url))}"
      # simply attempt to fetch from the access_url using ufs_from_url
      #  this supports various providers include http, ftp, and s3
      from ufs.url import ufs_from_url
      try:
        with ufs_from_url(access_url['url']) as ufs:
          yield from ufs.cat(access_url['url'])
        break
      except:
        pass
    raise RuntimeError(f"Failed to fetch object from any of the {info.get('access_methods')}")