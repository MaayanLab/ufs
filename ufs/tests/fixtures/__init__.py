import pytest
import pathlib

@pytest.fixture(params=[
  p.stem
  for p in pathlib.Path(__file__).parent.glob('[!_]*.py')
])
def ufs(request):
  ''' Load different ufs implementations from fixtures directory to be tested uniformly
  '''
  import importlib
  yield from importlib.import_module(f"ufs.tests.fixtures.{request.param}").ufs()
