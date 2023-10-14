''' A cache provider which will return cached results of the
resolve function for at most ttl seconds.
'''
import time
import typing as t
import dataclasses

T = t.TypeVar('T')

@dataclasses.dataclass
class TTLValue:
  val: t.Optional[t.Any] = None
  ttl: float = 0

class TTLCacheStore(t.Generic[T]):
  def __init__(self, ttl=60):
    self._ttl = ttl
    self._cache: t.Dict[str, TTLValue] = {}
  
  def __getitem__(self, key: str) -> t.Any:
    item = self._cache[key]
    if time.time() > item.ttl:
      self._cache.pop(key)
      raise KeyError(key)
    return item.val

  def __setitem__(self, key: str, val: T):
    self._cache[key] = TTLValue(
      val=val,
      ttl=time.time()+self._ttl,
    )

  def discard(self, key: str):
    self._cache.pop(key, None)

@dataclasses.dataclass
class Result:
  err: t.Optional[t.Any] = None
  val: t.Optional[t.Any] = None

class TTLCache(t.Generic[T]):
  def __init__(self, resolve: t.Callable[[str], T], ttl=60):
    self._resolve = resolve
    self._store = TTLCacheStore(ttl=ttl)

  def __call__(self, key: str) -> T:
    try:
      item = self._store[key]
    except KeyError:
      try:
        item = Result(val=self._resolve(key))
      except Exception as err:
        item = Result(err=err)
      self._store[key] = item
    if item.err is not None:
      raise item.err
    else:
      return item.val

  def __setitem__(self, key: str, val: T):
    self._store[key] = Result(val=val)

  def discard(self, key: str):
    self._store.discard(key)
