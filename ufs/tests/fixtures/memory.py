def ufs():
  from ufs.impl.memory import Memory
  with Memory() as ufs:
    yield ufs
