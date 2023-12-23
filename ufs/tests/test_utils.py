def test_url_parse():
  from ufs.utils.url import parse_url, parse_netloc
  url_parsed = parse_url('https://user:pass@host:1000/path#?test=true')
  assert url_parsed == dict(
    proto='https',
    path='user:pass@host:1000/path',
    fragment='?test=true',
  )
  netloc_parsed = parse_netloc(url_parsed)
  assert netloc_parsed == dict(
    netloc='user:pass@host:1000',
    username='user',
    password='pass',
    host='host',
    port=1000,
    path='/path',
  )
  url_parsed = parse_url('https://user@host:1000')
  assert url_parsed == dict(
    proto='https',
    path='user@host:1000',
    fragment=None,
  )
  netloc_parsed = parse_netloc(url_parsed)
  assert netloc_parsed == dict(
    netloc='user@host:1000',
    username='user',
    password=None,
    host='host',
    port=1000,
    path='',
  )

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

def test_pathlib_rmtree():
  import pathlib
  import tempfile
  from ufs.utils.pathlib import rmtree
  with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = pathlib.Path(tmpdir)
    (tmpdir/'a').write_text('b')
    (tmpdir/'c').mkdir()
    (tmpdir/'c'/'d').write_text('e')
    (tmpdir/'c'/'f').mkdir()
    (tmpdir/'c'/'f'/'g').write_text('h')
    rmtree(tmpdir/'c')
    assert [p.name for p in tmpdir.iterdir()] == ['a']
