''' Use fuse if libfuse is available, otherwise fallback to ffuse for mounting.
'''

import contextlib
from ufs.spec import UFS

@contextlib.contextmanager
def mount(ufs: UFS, mount_dir: str = None, readonly: bool = False):
  try:
    from ufs.access.fuse import fuse_mount as _mount
  except ImportError:
    import logging; logging.getLogger(__name__).warning('Install fusepy for proper fuse mounting, falling back to ffuse')
    from ufs.access.ffuse import ffuse_mount as _mount
  except OSError:
    import logging; logging.getLogger(__name__).warning('Install libfuse for proper fuse mounting, falling back to ffuse')
    from ufs.access.ffuse import ffuse_mount as _mount
  with _mount(ufs, mount_dir, readonly=readonly) as ufs:
    yield ufs
