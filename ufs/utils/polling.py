
def safe_predicate(predicate):
  try: return predicate()
  except: return False

def wait_for(predicate, interval=0.1, timeout=2.0):
  import time
  while not predicate():
    time.sleep(interval)
    timeout -= interval
    if timeout <= 0: raise TimeoutError()
