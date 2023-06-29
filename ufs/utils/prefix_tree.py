'''
A prefix tree for identification of the correct
 split given a list of path prefixes.
'''
from ufs.utils.pathlib import SafePurePosixPath, PathLike, coerce_pathlike

def create_prefix_tree_from_paths(paths: list[PathLike]):
  ''' The prefix tree turns a flat listing
  `a/b/c`
  into
  `{ 'a': { 'b': { None: 'c' } } }
  The special key `None` is used to carry a "fallback"
  (for any higher level stores which get carried forward)
  so `a/b/c` & `a` will result in
  `{ 'a': { None: 'a', 'b': { None: 'a', 'c': { None: 'a/b/c' } } }
  '''
  tree = {None: None}
  for path in map(SafePurePosixPath, paths):
    *front_paths, last_path = path.parts
    tmp = tree
    for p in front_paths:
      if p not in tmp:
        tmp[p] = {None: tmp[None]}
      tmp = tmp[p]
    tmp[last_path] = {None: path}
  return tree

@coerce_pathlike
def search_prefix_tree(tree, path: PathLike):
  ''' Given a search, we'll produce the most applicable path prefix and the subpath after that prefix
  e.g.
  assuming a prefix tree of:
  a/b/c
  a/
  a search for a/b will give a/, b
  a search for a/b/c/d/e will give a/b/c, d/e
  '''
  for i, part in enumerate(path.parts):
    if part in tree:
      tree = tree[part]
      if len(tree) == 1:
        return tree[None], SafePurePosixPath().joinpath(*path.parts[i+1:])
    else:
      break
  return tree[None], path

@coerce_pathlike
def list_prefix_tree(tree, path: PathLike):
  ''' Given a search, we'll provide the fallback path along with any additional prefixes
  that might be applicable at that level.
  :returns: primary_prefix, subpath_in_primary_prefix, {other_subpaths at this level} or None
  '''
  for i, part in enumerate(path.parts):
    if part in tree:
      tree = tree[part]
      if len(tree) == 1:
        return tree[None], SafePurePosixPath().joinpath(*path.parts[i+1:]), None
    else:
      return tree[None], path, None
  return tree[None], path, tree.keys() - {None}
