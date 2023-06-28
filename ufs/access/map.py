''' fsspec style dict mapper repr of a UFS
'''

import typing as t
from collections.abc import Mapping, MutableMapping
from ufs.spec import UFS
from ufs.access.pathlib import UPath
from ufs.utils.pathlib import rmtree

class UMap(MutableMapping):
  def __init__(self, ufs: UFS = None, upath: UPath = None):
    self._upath = UPath(ufs) if upath is None else upath

  def __repr__(self):
    return f'''UMap({repr(self._upath)}, {repr({
      key: self[key]
      for key in self
    })})'''

  def __getitem__(self, key: str) -> t.Union[str, 'UMap']:
    if (self._upath/key).is_file():
      return (self._upath/key).read_text()
    elif (self._upath/key).is_dir():
      return UMap(upath=self._upath/key)
    else:
      raise KeyError(key)

  def __setitem__(self, key: str, value: t.Union[str, Mapping]):
    if (self._upath/key).exists():
      if (self._upath/key).is_file(): (self._upath/key).unlink()
      elif (self._upath/key).is_dir(): rmtree(self._upath/key)
    if type(value) == str:
      (self._upath/key).write_text(value)
    elif isinstance(value, Mapping):
      (self._upath/key).mkdir()
      submap = UMap(upath=self._upath/key)
      for k, v in value.items():
        submap[k] = v
    else:
      raise NotImplementedError(value)

  def __delitem__(self, key: str) -> None:
    if (self._upath/key).is_file():
      return (self._upath/key).unlink()
    elif (self._upath/key).is_dir():
      return rmtree(self._upath/key)
    else:
      raise KeyError(key)

  def __iter__(self):
    for item in self._upath.iterdir():
      yield item.name

  def __len__(self) -> int:
    return sum(1 for _ in self._upath.iterdir())
