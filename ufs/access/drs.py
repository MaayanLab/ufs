''' Serve ufs using a GA4GH compatible DRS server where ids are content-addressable

- all files have their sha256sum computed
- all directories are the sha256sum of the concatenated sha256sum of their contents
- it's all served as a drs-compatible access endpoint
'''

import os
import re
import sys
import flask
import json
import typing as t
import datetime as dt
import contextlib
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

def index_ufs_for_drs(ufs: UFS, index: t.MutableMapping[str, t.Any] = {}):
  from ufs.access.shutil import walk
  index['objects'] = {}
  objects = index['objects']
  index['bundles'] = {}
  bundles = index['bundles']
  index['sha256sums'] = {}
  sha256sums = index['sha256sums']
  for path, info in walk(ufs, '/', dirfirst=False):
    if info['type'] == 'file':
      sha256sums[str(path)] = sha256sum = sha256(ufs.cat(path))
      objects[sha256sum] = dict(path=path, info=info)
    elif info['type'] == 'directory':
      if str(path) not in bundles: continue # this would happen with an empty directory (don't make empty bundles)
      sha256sums[str(path)] = sha256sum = sha256(map(str.encode, bundles[str(path)]))
      objects[sha256sum] = dict(path=path, info=info)
    if path != path.parent: # don't add root to itself
      if str(path.parent) not in bundles:
        bundles[str(path.parent)] = []
      bundles[str(path.parent)].append(sha256sum)
  return index

def flask_ufs_for_drs(ufs: UFS, index: t.Mapping[str, t.Any], *, app: t.Union[flask.Flask, flask.Blueprint], public_url: str):
  objects = index['objects']
  bundles = index['bundles']
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
    expand = json.loads(flask.request.args.get('expand', 'false'))
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
      # add "contents" to data when applicable, expanding
      #  children when expand=true param was specified
      Q = [data]
      while Q:
        item = Q.pop()
        item_object = objects[item['id']]
        if item_object['info']['type'] == 'directory':
          item['contents'] = contents = [
            { "id": id, "name": objects[id]['path'].name }
            for id in bundles[str(item_object['path'])]
          ]
          if expand: Q += contents
    return data
  @app.get('/ga4gh/drs/v1/objects/<object_id>/access/<access_id>')
  def objects_access_get(object_id, access_id):
    if access_id != 'https':
      return flask.abort(404)
    try:
      drs_object = objects[object_id]
    except KeyError:
      return flask.abort(404)
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

def create_app():
  import atexit
  ufs = UFS.from_dict(**json.loads(os.environ.pop('UFS_SPEC')))
  ufs.start()
  @atexit.register
  def cleanup():
    ufs.stop()
  return flask_ufs_for_drs(
    ufs, index_ufs_for_drs(ufs),
    app=flask.Flask(__name__),
    public_url=os.environ.pop('UFS_PUBLIC_URL'),
  )

@contextlib.contextmanager
def serve_ufs_via_drs(ufs: UFS, host: str, port: int = 80, public_url: str = None):
  import shutil
  import signal
  from subprocess import Popen
  from ufs.utils.process import active_process
  from ufs.utils.polling import wait_for
  from ufs.utils.socket import nc_z
  if public_url is None:
    if port == 80:
      public_url = f"http://{host}"
    else:
      public_url = f"http://{host}:{port}"
  gunicorn = shutil.which('gunicorn')
  assert gunicorn
  with active_process(Popen(
    [gunicorn, '--bind', f"{host}:{port}", 'ufs.access.drs:create_app()'],
    stdout=sys.stdout,
    stderr=sys.stderr,
    env=dict(os.environ,
      UFS_SPEC=json.dumps(ufs.to_dict()),
      UFS_PUBLIC_URL=public_url,
    ),
  ), terminate_signal=signal.SIGINT):
    wait_for(lambda: nc_z(host, port))
    yield

if __name__ == '__main__':
  os.execv(
    sys.executable,
    [sys.executable, '-m', 'gunicorn', *sys.argv[1:], 'ufs.access.drs:create_app()']
  )
