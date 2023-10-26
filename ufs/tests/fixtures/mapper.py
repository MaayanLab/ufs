def ufs():
  from ufs.impl.memory import Memory
  from ufs.impl.mapper import Mapper
  with Mapper({
    '/': Memory(),
  }) as ufs:
    yield ufs
