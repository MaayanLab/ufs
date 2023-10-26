def ufs():
  from ufs.impl.process import Process
  from ufs.impl.memory import Memory
  with Process(Memory()) as ufs:
    yield ufs
