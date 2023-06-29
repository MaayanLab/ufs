''' Serve ufs using a DRS server -- ids are content-addressable

- all files have their sha256sum computed
- all directories are the sha256sum of the concatenated sha256sum of their contents
- it's all served as a drs-compatible access endpoint
'''

import re
import flask
import datetime as dt
from ufs.spec import UFS

def sha256(stream):
  import hashlib
  h = hashlib.sha256()
  for buf in stream:
    h.update(buf)
  return h.hexdigest()

def RFC3339(ts = None):
  if ts is not None:
    ts = dt.datetime.fromtimestamp(ts)
  else:
    ts = dt.datetime.now()
  return ts.strftime('%Y-%m-%dT%H:%M:%SZ')

def drs_from_ufs(ufs: UFS, *, app: flask.Blueprint, public_url: str):
  from ufs.access.shutil import walk
  objects = {}
  bundles = {}
  sha256sums = {}
  for path, info in walk(ufs, '/', dirfirst=False):
    if info['type'] == 'file':
      sha256sums[path] = sha256sum = sha256(ufs.cat(path))
      objects[sha256sum] = dict(path=path, info=info)
      if path.parent not in bundles:
        bundles[path.parent] = []
      bundles[path.parent].push(sha256sum)
    elif info['type'] == 'directory':
      if path.parent in bundles:
        sha256sums[path] = sha256sum = sha256(bundles[path.parent])
        objects[sha256sum] = dict(path=path, info=info)
  #
  created_at = RFC3339()
  @app.get('/ga4gh/drs/v1/service-info')
  def service_info():
    return {
      "id": "cloud.maayanlab.ufs",
      "name": "UFS",
      "type": {
        "group": "org.ga4gh",
        "artifact": "drs",
        "version": "1.0.0"
      },
      "description": "This service provides DRS capabilities for UFS.",
      "organization": {
        "name": "Playbook Partnership",
        "url": "https://playbook-workflow-builder.cloud"
      },
      "contactUrl": "mailto:avi.maayan@mssm.edu",
      "documentationUrl": "https://github.com/nih-cfde/playbook-partnership",
      "createdAt": created_at,
      "environment": "test",
      "version": "1.0.0"
    }
  @app.get('/ga4gh/drs/v1/objects/<object_id>')
  def objects_get(object_id):
    try:
      drs_object = objects[object_id]
    except KeyError:
      return flask.abort(404)
    data = {
      "id": object_id,
      "name": drs_object['path'].name,
      "self_uri": f"{re.sub(r'/$', '', re.sub(r'^https', 'drs', public_url))}/{object_id}",
      "size": drs_object['info']['size'],
      "created_time": RFC3339(drs_object['info'].get('ctime')),
      "checksums": [
        {"type": "sha-256", "checksum": object_id},
      ],
    }
    if 'mtime' in drs_object['info']:
      data.update({
        "updated_time": RFC3339(drs_object['info']['mtime']),
      })
    #
    if drs_object['info']['type'] == 'file':
      data.update({
        # "mime_type": drs_object.mime_type,
        "access_methods": [
          {'type': 'https', 'access_id': 'https'},
          {'type': 'https', 'access_url': f"{public_url}/ga4gh/drs/v1/objects/{object_id}/data"},
        ],
      })
    elif drs_object['info']['type'] == 'directory':
      data.update({
        "contents": [
          { "id": id, "name": objects[id]['path'].name }
          for id in bundles[object_id]
        ],
      })
    return data
  @app.get('/ga4gh/drs/v1/objects/<object_id>/access/<access_id>')
  def objects_access_get(object_id, access_id):
    try:
      drs_object = objects[object_id]
    except KeyError:
      return flask.abort(404)
    assert access_id == 'https'
    return {
      "url": f"{public_url}/ga4gh/drs/v1/objects/{object_id}/data",
      # "headers": {},
    }
  @app.get('/ga4gh/drs/v1/objects/<object_id>/data')
  def objects_data_get(object_id):
    try:
      drs_object = objects[object_id]
    except KeyError:
      return flask.abort(404)
    try:
      return app.response_class(ufs.cat(drs_object['path']))
    except FileNotFoundError:
      flask.abort(404)
  #
  return app
