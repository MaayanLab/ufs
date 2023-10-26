def ufs():
  from ufs.impl.overlay import Overlay
  from ufs.impl.memory import Memory
  with Overlay(Memory(), Memory()) as ufs:
    yield ufs
