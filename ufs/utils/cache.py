''' A cache provider which will return cached results of the
resolve function for at most ttl seconds.
'''
import time
import typing as t
import dataclasses

T = t.TypeVar('T')

@dataclasses.dataclass
class CacheEntry:
  err: t.Optional[Exception] = None
  val: t.Optional[t.Any] = None
  ttl: float = 0

class TTLCache(t.Generic[T]):
  def __init__(self, resolve: t.Callable[[str], T], ttl=60):
    self._resolve = resolve
    self._ttl = ttl
    self._cache: dict[str, CacheEntry] = {}

  def __call__(self, key: str) -> T:
    item = self._cache.pop(key, None)
    if item is None or time.time() > item.ttl:
      try:
        item = CacheEntry(
          val=self._resolve(key),
          ttl=time.time()+self._ttl,
        )
      except Exception as err:
        item = CacheEntry(
          err=err,
          ttl=time.time()+self._ttl,
        )
    self._cache[key] = item
    if item.err is not None:
      raise item.err
    else:
      return item.val

  def __setitem__(self, key: str, val: T):
    self._cache[key] = CacheEntry(
      val=val,
      ttl=time.time()+self._ttl,
    )

  def discard(self, key: str):
    self._cache.pop(key, None)
