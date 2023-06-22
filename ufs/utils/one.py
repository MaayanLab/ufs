def one(items):
  it = iter(items)
  try: item = next(it)
  except StopIteration: raise
  try: next(it)
  except StopIteration: return item
  else: raise RuntimeError('Expected one, got multiple')
