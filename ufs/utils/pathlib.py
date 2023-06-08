import pathlib
import typing as t

class SafePurePosixPath_:
  def __init__(self, path: 'PathLike' = pathlib.PurePosixPath('/')):
    self._path = path
  def __truediv__(self, subpath: 'PathLike'):
    p = self._path
    sp = pathlib.PurePosixPath(subpath)
    for part in sp.parts:
      if part == '..': p = p.parent
      elif part in ['/', '.', '']: pass
      else: p = p / part
    return SafePurePosixPath_(p)
  def __getattr__(self, attr):
    return getattr(self._path, attr)
  def __str__(self) -> str:
    return str(self._path)
  def __repr__(self) -> str:
    return f"SafePurePosixPath({repr(str(self._path))})"

PathLike: t.TypeAlias = str | pathlib.PurePosixPath | SafePurePosixPath_

def SafePurePosixPath(path: PathLike) -> SafePurePosixPath_:
  ''' This ensures the path will always be /something
  It's not possible to go above the parent, // or /./ does nothing, etc..
  '''
  if isinstance(path, SafePurePosixPath_):
    return path
  return SafePurePosixPath_() / path

def pathparent(path: PathLike) -> SafePurePosixPath_:
  return SafePurePosixPath(path).parent

def pathname(path: PathLike) -> SafePurePosixPath_:
  return SafePurePosixPath(path).name

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
      if func_spec.annotations.get(argname) in [PathLike, SafePurePosixPath_]
    }
    return func(*args_, **kwargs_)
  setattr(wrapper, '__annotations__', {
    argname: SafePurePosixPath_ if argtype in [PathLike, SafePurePosixPath_] else argtype
    for argname, argtype in func.__annotations__.items()
  })
  return wrapper
