import pytest
import logging
logger = logging.getLogger(__name__)

@pytest.fixture(params=['http', 'https'])
def scheme(request):
  yield request.param

def test_http(scheme):
  from ufs.impl.http import HTTP
  from ufs.utils.pathlib import SafePurePosixPath
  with HTTP('google.com', scheme=scheme) as ufs:
    ret = ufs.info(SafePurePosixPath('/robots.txt'))
    logger.debug(ret)
    ret = b''.join(ufs.cat(SafePurePosixPath('/robots.txt')))
    logger.debug(ret)
    assert ret
