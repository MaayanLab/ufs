def test_prefix_tree():
  from ufs.utils.pathlib import SafePurePosixPath
  from ufs.utils.prefix_tree import create_prefix_tree_from_paths, search_prefix_tree, list_prefix_tree
  t = create_prefix_tree_from_paths([
    '',
    'a/b/c',
    'a/d',
  ])
  assert search_prefix_tree(t, '') == (SafePurePosixPath(), SafePurePosixPath())
  assert search_prefix_tree(t, 'a/d/e') == (SafePurePosixPath('a/d'), SafePurePosixPath('e'))
  assert search_prefix_tree(t, 'a/b/c') == (SafePurePosixPath('a/b/c'), SafePurePosixPath())
  assert search_prefix_tree(t, 'a/b/e') == (SafePurePosixPath(), SafePurePosixPath('a/b/e'))
  assert list_prefix_tree(t, '') == (SafePurePosixPath(), SafePurePosixPath(), {'a'})
  assert list_prefix_tree(t, 'a') == (SafePurePosixPath(), SafePurePosixPath('a'), {'b', 'd'})
  assert list_prefix_tree(t, 'b') == (SafePurePosixPath(), SafePurePosixPath('b'), None)
  assert list_prefix_tree(t, 'a/d/e') == (SafePurePosixPath('a/d'), SafePurePosixPath('e'), None)
