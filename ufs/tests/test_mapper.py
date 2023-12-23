def test_mapper():
  from ufs.impl.memory import Memory
  from ufs.impl.mapper import Mapper
  from ufs.access.pathlib import UPath
  root = Memory()
  sub = Memory()
  with Mapper({ '/': root, '/a/b': sub }) as mapper:
    root = UPath(root)
    (root/'a/b').mkdir(parents=True) # TODO
    sub = UPath(sub)
    mapper = UPath(mapper)
    assert {p.name for p in (mapper/'a').iterdir()} == {'b'}
    (mapper/'a/b/c').write_text('test2')
    (mapper/'a/d').write_text('test1')
    assert (sub/'c').read_text() == 'test2'
    assert (root/'a/d').read_text() == 'test1'

def test_singlefile_mapper():
  from ufs.impl.memory import Memory
  from ufs.impl.mapper import Mapper
  from ufs.impl.prefix import Prefix
  from ufs.access.pathlib import UPath
  backing = Memory()
  ufs = Mapper({ 'A': Prefix(backing, '/A') })
  with ufs:
    (UPath(backing) / 'A').write_text('a')
    (UPath(backing) / 'B').write_text('b')
    assert [p.name for p in UPath(ufs).iterdir()] == ['A']
    assert (UPath(ufs)/'A').read_text() == 'a'
