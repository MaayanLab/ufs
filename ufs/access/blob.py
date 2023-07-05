import os
import sys
import flask
import hashlib
from ufs.spec import UFS
from ufs.impl.tempdir import TemporaryDirectory
from ufs.utils.pathlib import SafePurePosixPath
from ufs.access.pathlib import UPath
from ufs.access.shutil import movefile

def flask_ufs_for_blob(ufs: UFS, tmpdir: UFS, *, app: flask.Flask | flask.Blueprint):
  @app.post('/ufs/blob/v1/objects')
  def objects_post():
    import uuid
    tmp = str(uuid.uuid4())
    h = hashlib.sha256()
    with (UPath(tmpdir)/tmp).open('wb') as fw:
      while True:
        buf = flask.request.stream.read(TemporaryDirectory.CHUNK_SIZE)
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
  def objects_get(object_id):
    if not (UPath(ufs)/object_id).is_file():
      flask.abort(404)
    return flask.Response(ufs.cat(SafePurePosixPath(object_id)))
  #
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
  )

if __name__ == '__main__':
  os.execv(
    sys.executable,
    [sys.executable, '-m', 'gunicorn', *sys.argv[1:], 'ufs.access.blob:create_app()']
  )
