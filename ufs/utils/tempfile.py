import pathlib
import tempfile
import contextlib
import typing as t

@contextlib.contextmanager
def TemporaryMountDirectory(mount_dir: t.Optional[str] = None):
  if mount_dir is None:
    mount_dir_resolved = pathlib.Path(tempfile.mkdtemp())
  else:
    mount_dir_resolved = pathlib.Path(mount_dir)
    if mount_dir_resolved.exists():
      mount_dir = str(mount_dir_resolved)
    else:
      mount_dir_resolved.mkdir(parents=True)
  try:
    yield mount_dir_resolved
  finally:
    if mount_dir is None and mount_dir_resolved.exists() and not mount_dir_resolved.is_mount():
      mount_dir_resolved.rmdir()
