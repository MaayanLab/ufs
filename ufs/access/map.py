''' fsspec style dict mapper repr of a UFS
'''

import typing as t
import urllib.parse
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
    name = urllib.parse.quote(str(key), safe='')
    if (self._upath/name).is_file():
      return (self._upath/name).read_text()
    elif (self._upath/name).is_dir():
      return UMap(upath=self._upath/name)
    else:
      raise KeyError(key)

  def __setitem__(self, key: str, value: t.Union[str, Mapping]):
    name = urllib.parse.quote(str(key), safe='')
    if (self._upath/name).exists():
      if (self._upath/name).is_file(): (self._upath/name).unlink()
      elif (self._upath/name).is_dir(): rmtree(self._upath/name)
    if type(value) == str:
      (self._upath/name).write_text(value)
    elif isinstance(value, Mapping):
      (self._upath/name).mkdir()
      submap = UMap(upath=self._upath/name)
      for k, v in value.items():
        submap[k] = v
    else:
      raise NotImplementedError(value)

  def __delitem__(self, key: str) -> None:
    name = urllib.parse.quote(str(key), safe='')
    if (self._upath/name).is_file():
      return (self._upath/name).unlink()
    elif (self._upath/name).is_dir():
      return rmtree(self._upath/name)
    else:
      raise KeyError(key)

  def __iter__(self):
    for item in self._upath.iterdir():
      yield urllib.parse.unquote(item.name)

  def __contains__(self, key: str):
    name = urllib.parse.quote(str(key), safe='')
    return (self._upath/name).exists()

  def __len__(self) -> int:
    return sum(1 for _ in self._upath.iterdir())
