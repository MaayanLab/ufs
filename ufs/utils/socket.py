import socket

def nc_z(host: str, port: int, timeout: int = 1):
  ''' Like nc -z but in python -- i.e. check if a tcp connection gets established
  (i.e. port open) but do nothing else.
  '''
  try:
    with socket.create_connection((host, port), timeout=timeout):
      return True
  except KeyboardInterrupt:
    raise
  except:
    return False

def autosocket(host='', port=0):
  with socket.socket() as s:
    s.bind((host, port))
    host, port = s.getsockname()
  return host, port
