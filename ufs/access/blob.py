import os
import re
import sys
import flask
import hashlib
import datetime as dt
import typing as t
from ufs.spec import UFS
from ufs.impl.tempdir import TemporaryDirectory
from ufs.utils.pathlib import SafePurePosixPath
from ufs.access.pathlib import UPath
from ufs.access.shutil import movefile

def RFC3339(ts = None):
  if ts is not None:
    ts = dt.datetime.fromtimestamp(ts)
  else:
    ts = dt.datetime.now()
  return ts.strftime('%Y-%m-%dT%H:%M:%SZ')

def flask_ufs_for_blob(ufs: UFS, tmpdir: UFS, *, app: t.Union[flask.Flask, flask.Blueprint], public_url: str):
  created_at = RFC3339()
  @app.post('/ufs/blob/v1/objects')
  def blob_objects_post():
    import uuid
    tmp = str(uuid.uuid4())
    h = hashlib.sha256()
    with (UPath(tmpdir)/tmp).open('wb') as fw:
      while True:
        buf = flask.request.stream.read(tmpdir.CHUNK_SIZE)
        if not buf:
          break
        h.update(buf)
        fw.write(buf)
    object_id = h.hexdigest()
    if not (UPath(ufs)/object_id).exists():
      movefile(tmpdir, tmp, ufs, object_id)
    else:
      (UPath(tmpdir)/tmp).unlink()
    return flask.jsonify(object_id)
  #
  @app.get('/ufs/blob/v1/objects/<object_id>')
  def blob_objects_get(object_id):
    if not (UPath(ufs)/object_id).is_file():
      flask.abort(404)
    return flask.Response(ufs.cat(SafePurePosixPath(object_id)))
  #
  @app.get('/ga4gh/drs/v1/service-info')
  def drs_service_info():
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
  def drs_objects_get(object_id):
    try:
      info = ufs.info(SafePurePosixPath(object_id))
    except FileNotFoundError:
      return flask.abort(404)
    return {
      "id": object_id,
      "name": object_id,
      "self_uri": f"{re.sub(r'/$', '', re.sub(r'^https', 'drs', public_url))}/{object_id}",
      "size": info['size'],
      "created_time": RFC3339(info.get('ctime')),
      "checksums": [
        {"type": "sha-256", "checksum": object_id},
      ],
      "access_methods": [
        {'type': 'https', 'access_id': 'https'},
        {'type': 'https', 'access_url': f"{public_url}/ufs/blob/v1/objects/{object_id}"},
      ],
    }
  @app.get('/ga4gh/drs/v1/objects/<object_id>/access/<access_id>')
  def drs_objects_access_get(object_id, access_id):
    if access_id != 'https':
      return flask.abort(404)
    try:
      info = ufs.info(SafePurePosixPath(object_id))
    except FileNotFoundError:
      return flask.abort(404)
    return {
      "url": f"{public_url}/ufs/blob/v1/objects/{object_id}",
      # "headers": {},
    }
  return app

def create_app():
  import json
  import atexit
  ufs = UFS.from_dict(**json.loads(os.environ.pop('UFS_SPEC')))
  ufs.start()
  tmpdir = TemporaryDirectory()
  tmpdir.start()
  @atexit.register
  def cleanup():
    tmpdir.stop()
    ufs.stop()
  return flask_ufs_for_blob(
    ufs, tmpdir,
    app=flask.Flask(__name__),
    public_url=os.environ.pop('UFS_PUBLIC_URL'),
  )

if __name__ == '__main__':
  os.execv(
    sys.executable,
    [sys.executable, '-m', 'gunicorn', *sys.argv[1:], 'ufs.access.blob:create_app()']
  )
