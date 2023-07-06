import pathlib
import typing as t

class SafePurePosixPath_:
  def __init__(self, path: pathlib.PurePosixPath = pathlib.PurePosixPath('/')):
    self._path = path
  def __reduce__(self):
    return (self.__class__, (self._path,),)
  def __hash__(self):
    return hash(self._path)
  def __eq__(self, other):
    return hash(self) == hash(other)
  @property
  def parent(self):
    return SafePurePosixPath_(self._path.parent)
  @staticmethod
  def _from_parts(parts):
    root, *ps = parts
    return root + '/'.join(ps)
  def joinpath(self, *other):
    p = self
    for op in other:
      p = p / op
    return p
  def __truediv__(self, subpath: 'PathLike'):
    p = self._path
    sp = subpath if isinstance(subpath, pathlib.PurePosixPath) or isinstance(subpath, SafePurePosixPath_) else pathlib.PurePosixPath(str(subpath))
    for part in sp.parts:
      if part == '..': p = p.parent
      elif part in ['//', '/', '.', '']: pass
      else: p = p / part
    return SafePurePosixPath_(p)
  def relative_to(self, parentpath: 'PathLike'):
    if parentpath.parts != self.parts[:len(parentpath.parts)]:
      raise RuntimeError('Not relative')
    else:
      return SafePurePosixPath_._from_parts(('./', *self.parts[len(parentpath.parts):]))
  def __getattr__(self, attr):
    return getattr(self._path, attr)
  def __str__(self) -> str:
    return str(self._path)
  def __repr__(self) -> str:
    return f"SafePurePosixPath({repr(str(self._path))})"

PathLike: t.TypeAlias =  bytes | str | pathlib.PurePosixPath | SafePurePosixPath_

def SafePurePosixPath(path: PathLike = None) -> SafePurePosixPath_:
  ''' This ensures the path will always be /something
  It's not possible to go above the parent, // or /./ does nothing, etc..
  '''
  if not path:
    return SafePurePosixPath_()
  if isinstance(path, bytes):
    path = str(path)
  if isinstance(path, SafePurePosixPath_):
    return path
  return SafePurePosixPath_() / path

def pathparent(path: str):
  parent, sep, _name = str(path).rstrip('/').rpartition('/')
  if parent == '': return sep or ''
  else: return parent

def pathname(path: str):
  _parent, sep, name = str(path).rstrip('/').rpartition('/')
  if not name: return sep or ''
  else: return name

def coerce_pathlike(func: t.Callable):
  ''' Decorate functions with this and all PathLike annotated parameters become SafePurePosixPaths
  '''
  import functools, inspect
  func_spec = inspect.getfullargspec(func)
  @functools.wraps(func)
  def wrapper(*args, **kwargs):
    args_ = [
      SafePurePosixPath(argvalue) if func_spec.annotations.get(argname) in [PathLike, SafePurePosixPath_] else argvalue
      for argname, argvalue in zip(func_spec.args, args)
    ]
    kwargs_ = {
      argname: SafePurePosixPath(argvalue) if func_spec.annotations.get(argname) in [PathLike, SafePurePosixPath_] else argvalue
      for argname, argvalue in kwargs.items()
    }
    return func(*args_, **kwargs_)
  setattr(wrapper, '__annotations__', {
    argname: SafePurePosixPath_ if argtype in [PathLike, SafePurePosixPath_] else argtype
    for argname, argtype in func.__annotations__.items()
  })
  return wrapper

def rmtree(P: pathlib.Path):
  ''' This doesn't exist in normal pathlib but comes in handy
  '''
  Q = [(P, True)] + [(path, False) for path in P.iterdir()]
  while Q:
    path, empty = Q.pop()
    if path.is_file(): path.unlink()
    elif path.is_dir():
      if empty: path.rmdir()
      else: Q += [(path, True)] + [(p, False) for p in path.iterdir()]
