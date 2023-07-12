''' shutil-style high level file ops between UFS stores
'''
from ufs.spec import UFS, AsyncUFS
from ufs.utils.pathlib import SafePurePosixPath_, coerce_pathlike

@coerce_pathlike
def walk(ufs: UFS, path: SafePurePosixPath_, dirfirst=True):
  '''
  :params dirfirst: Controls whether the directories are yielded before or after the files in the directory

  dirfirst=True: / /a/ /a/b /a/c ...
  dirfirst=False: ... /a/b /a/c /a/ /
  '''
  info = ufs.info(path)
  if info['type'] == 'directory':
    Q = []
    if dirfirst:
      yield path, info
    else:
      Q += [(path, True)]
    Q += [(path/p, False) for p in ufs.ls(path)]
    while Q:
      p, empty = Q.pop()
      i = ufs.info(p)
      if i['type'] == 'file':
        yield p, i
      elif i['type'] == 'directory':
        if empty:
          yield p, i
        else:
          if dirfirst:
            yield p, i
          else:
            Q += [(p, True)]
          Q += [(p/pp, False) for pp in ufs.ls(p)]
  else:
    yield path, info

@coerce_pathlike
async def async_walk(ufs: AsyncUFS, path: SafePurePosixPath_, dirfirst=True):
  '''
  :params dirfirst: Controls whether the directories are yielded before or after the files in the directory

  dirfirst=True: / /a/ /a/b /a/c ...
  dirfirst=False: ... /a/b /a/c /a/ /
  '''
  info = await ufs.info(path)
  if info['type'] == 'directory':
    Q = []
    if dirfirst:
      yield path, info
    else:
      Q += [(path, True)]
    Q += [(path/p, False) for p in await ufs.ls(path)]
    while Q:
      p, empty = Q.pop()
      i = await ufs.info(p)
      if i['type'] == 'file':
        yield p, i
      elif i['type'] == 'directory':
        if empty:
          yield p, i
        else:
          if dirfirst:
            yield p, i
          else:
            Q += [(p, True)]
          Q += [(p/pp, False) for pp in await ufs.ls(p)]
  else:
    yield path, info

@coerce_pathlike
def copyfile(src_ufs: UFS, src_path: SafePurePosixPath_, dst_ufs: UFS, dst_path: SafePurePosixPath_):
  if src_ufs is dst_ufs:
    src_ufs.copy(src_path, dst_path)
  else:
    src_fd = src_ufs.open(src_path, 'rb')
    dst_fd = dst_ufs.open(dst_path, 'wb')
    while buf := src_ufs.read(src_fd, src_ufs.CHUNK_SIZE):
      dst_ufs.write(dst_fd, buf)
    dst_ufs.close(dst_fd)
    src_ufs.close(src_fd)

@coerce_pathlike
async def async_copyfile(src_ufs: AsyncUFS, src_path: SafePurePosixPath_, dst_ufs: UFS, dst_path: SafePurePosixPath_):
  if src_ufs is dst_ufs:
    await src_ufs.copy(src_path, dst_path)
  else:
    src_fd = await src_ufs.open(src_path, 'rb')
    dst_fd = await dst_ufs.open(dst_path, 'wb')
    while buf := await src_ufs.read(src_fd, src_ufs.CHUNK_SIZE):
      await dst_ufs.write(dst_fd, buf)
    await dst_ufs.close(dst_fd)
    await src_ufs.close(src_fd)

@coerce_pathlike
def movefile(src_ufs: UFS, src_path: SafePurePosixPath_, dst_ufs: UFS, dst_path: SafePurePosixPath_):
  if src_ufs is dst_ufs:
    if str(src_path) in str(dst_path):
      raise RuntimeError("Can't move path into itself")
    src_ufs.rename(src_path, dst_path)
  else:
    copyfile(src_ufs, src_path, dst_ufs, dst_path)
    src_ufs.unlink(src_path)

@coerce_pathlike
async def movefile(src_ufs: AsyncUFS, src_path: SafePurePosixPath_, dst_ufs: UFS, dst_path: SafePurePosixPath_):
  if src_ufs is dst_ufs:
    if str(src_path) in str(dst_path):
      raise RuntimeError("Can't move path into itself")
    await src_ufs.rename(src_path, dst_path)
  else:
    await async_copyfile(src_ufs, src_path, dst_ufs, dst_path)
    await src_ufs.unlink(src_path)

@coerce_pathlike
def copytree(src_ufs: UFS, src_path: SafePurePosixPath_, dst_ufs: UFS, dst_path: SafePurePosixPath_, exists_ok=False):
  for p, i in walk(src_ufs, src_path, dirfirst=True):
    rel_path = p.relative_to(src_path)
    if i['type'] == 'directory':
      try:
        dst_ufs.mkdir(dst_path / rel_path)
      except FileExistsError:
        if not exists_ok:
          raise
    elif i['type'] == 'file':
      copyfile(src_ufs, p, dst_ufs, dst_path / rel_path)

@coerce_pathlike
async def async_copytree(src_ufs: AsyncUFS, src_path: SafePurePosixPath_, dst_ufs: UFS, dst_path: SafePurePosixPath_, exists_ok=False):
  async for p, i in async_walk(src_ufs, src_path, dirfirst=True):
    rel_path = p.relative_to(src_path)
    if i['type'] == 'directory':
      try:
        await dst_ufs.mkdir(dst_path / rel_path)
      except FileExistsError:
        if not exists_ok:
          raise
    elif i['type'] == 'file':
      await async_copyfile(src_ufs, p, dst_ufs, dst_path / rel_path)

@coerce_pathlike
def copy(src_ufs: UFS, src_path: SafePurePosixPath_, dst_ufs: UFS, dst_path: SafePurePosixPath_):
  src_info = src_ufs.info(src_path)
  try:
    dst_info = dst_ufs.info(dst_path)
  except FileNotFoundError:
    dst_info = { 'type': 'file' }
  if dst_info['type'] == 'directory':
    dst_path = dst_path / src_path.name
  if src_info['type'] == 'directory':
    copytree(src_ufs, src_path, dst_ufs, dst_path)
  else:
    copyfile(src_ufs, src_path, dst_ufs, dst_path)

@coerce_pathlike
async def async_copy(src_ufs: AsyncUFS, src_path: SafePurePosixPath_, dst_ufs: UFS, dst_path: SafePurePosixPath_):
  src_info = await src_ufs.info(src_path)
  try:
    dst_info = await dst_ufs.info(dst_path)
  except FileNotFoundError:
    dst_info = { 'type': 'file' }
  if dst_info['type'] == 'directory':
    dst_path = dst_path / src_path.name
  if src_info['type'] == 'directory':
    await async_copytree(src_ufs, src_path, dst_ufs, dst_path)
  else:
    await async_copyfile(src_ufs, src_path, dst_ufs, dst_path)

@coerce_pathlike
def rmtree(ufs: UFS, path: SafePurePosixPath_):
  for p, i in walk(ufs, path, dirfirst=False):
    if i['type'] == 'file':
      ufs.unlink(p)
    elif i['type'] == 'directory':
      ufs.rmdir(p)

@coerce_pathlike
async def async_rmtree(ufs: AsyncUFS, path: SafePurePosixPath_):
  async for p, i in async_walk(ufs, path, dirfirst=False):
    if i['type'] == 'file':
      await ufs.unlink(p)
    elif i['type'] == 'directory':
      await ufs.rmdir(p)

@coerce_pathlike
def move(src_ufs: UFS, src_path: SafePurePosixPath_, dst_ufs: UFS, dst_path: SafePurePosixPath_):
  if src_ufs is dst_ufs and str(src_path) in str(dst_path):
    raise RuntimeError("Can't move path into itself")
  copy(src_ufs, src_path, dst_ufs, dst_path)
  rmtree(src_ufs, src_path)

@coerce_pathlike
async def async_move(src_ufs: AsyncUFS, src_path: SafePurePosixPath_, dst_ufs: UFS, dst_path: SafePurePosixPath_):
  if src_ufs is dst_ufs and str(src_path) in str(dst_path):
    raise RuntimeError("Can't move path into itself")
  await async_copy(src_ufs, src_path, dst_ufs, dst_path)
  await async_rmtree(src_ufs, src_path)
