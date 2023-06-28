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
