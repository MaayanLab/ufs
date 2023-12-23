def test_overlay():
  from ufs.impl.memory import Memory
  from ufs.impl.overlay import Overlay
  from ufs.access.pathlib import UPath
  lower = Memory()
  upper = Memory()
  with Overlay(lower, upper) as overlay:
    lower = UPath(lower)
    upper = UPath(upper)
    overlay = UPath(overlay)
    (overlay / 'test_dir').mkdir()
    (lower / 'test').write_text('Hello World')
    assert {p.name for p in overlay.iterdir()} == {'test', 'test_dir'}
    (overlay / 'test2').write_text('Hello World!')
    assert {p.name for p in lower.iterdir()} == {'test'}
    assert {p.name for p in upper.iterdir()} == {'test2', 'test_dir'}
    assert {p.name for p in overlay.iterdir()} == {'test', 'test2', 'test_dir'}
    (overlay / 'test').write_text('Hello World!')
    assert (lower / 'test').read_text() == 'Hello World'
    assert (overlay / 'test').read_text() == 'Hello World!'
