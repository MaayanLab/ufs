import os
import sys
import flask
import datetime as dt
import typing as t
import urllib.parse
from ufs.spec import UFS
from ufs.impl.tempdir import TemporaryDirectory
from ufs.utils.pathlib import SafePurePosixPath

def RFC3339(ts = None):
  if ts is not None:
    ts = dt.datetime.fromtimestamp(ts)
  else:
    ts = dt.datetime.now()
  return ts.strftime('%Y-%m-%dT%H:%M:%SZ')

def directory_listing(ufs: UFS, path):
  yield '<ul>'
  for f in ufs.ls(path):
    yield f'<li><a href="{path / f}">{f}</a></li>'
  yield '</ul>'

def flask_ufs_for_http(ufs: UFS, *, app: t.Union[flask.Flask, flask.Blueprint]):
  @app.route('/', defaults={'path': ''})
  @app.get('/<path:path>')
  def http_get(path):
    try:
      path = SafePurePosixPath(f"/{path}")
      info = ufs.info(path)
      if info['type'] == 'file':
        return flask.Response(ufs.cat(path))
      else:
        return flask.Response(directory_listing(ufs, path))
    except FileNotFoundError:
      return flask.abort(404)
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
    ufs.stop()
  return flask_ufs_for_http(
    ufs,
    app=flask.Flask(__name__),
    # public_url=os.environ.pop('UFS_PUBLIC_URL', None),
  )

if __name__ == '__main__':
  os.execv(
    sys.executable,
    [sys.executable, '-m', 'gunicorn', *sys.argv[1:], 'ufs.access.http:create_app()']
  )
