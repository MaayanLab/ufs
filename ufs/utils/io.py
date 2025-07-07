''' Some custom IO Base generics since the core python ones are a bit odd
'''

class Buffer:
  def __init__(self) -> None:
    self.pos = 0
    self.data = b''

  def write(self, data: bytes):
    self.data += data

  def trim(self, pos):
    if self.pos < pos:
      self.data = self.data[pos - self.pos:]
    elif self.pos > pos:
      self.data = b''

  def read(self, amnt = -1):
    ret = self.data if amnt == -1 else self.data[:amnt]
    self.pos += len(ret)
    self.data = self.data[len(ret):]
    return ret

  def read_until(self, delim):
    left, sep, right = self.data.partition(delim)
    if not sep:
      self.pos += len(left)
      self.data = b''
      return left, False
    else:
      self.pos += len(left)+len(sep)
      self.data = right
      return left+sep, True


class RawBinaryIO:
  def seek(self, amnt: int, whence: int = 0):
    raise NotImplementedError()
  def read(self, amnt = -1) -> bytes:
    raise NotImplementedError()
  def write(self, data: bytes) -> int:
    raise NotImplementedError()
  def flush(self):
    pass
  def close(self):
    raise NotImplementedError()
  def truncate(self, length: int = None):
    raise NotImplementedError()
  def tell(self) -> int:
    raise NotImplementedError()

class BufferedBinaryIO:
  def __init__(self, raw: RawBinaryIO, chunk_size = 4096, newline = b'\n') -> None:
    self.raw = raw
    self.pos = 0
    self.chunk_size = chunk_size
    self.read_buffer = Buffer()
    self.newline = newline
    self.closed = False

  def seek(self, amnt: int, whence: int = 0):
    assert not self.closed
    self.raw.seek(amnt, whence)
    if whence == 0: self.pos = amnt
    elif whence == 1: self.pos += amnt
    elif whence == 2: self.pos = -amnt
    self.read_buffer.trim(self.pos)

  def read(self, amnt = -1) -> bytes:
    assert not self.closed
    if amnt == 0:
      return b''
    ret = self.read_buffer.read(amnt)
    if amnt == -1:
      ret += self.raw.read(-1)
    elif len(ret) < amnt:
      ret += self.raw.read(amnt - len(ret))
    self.pos += len(ret)
    return ret

  def write(self, data: bytes) -> int:
    assert not self.closed
    ret = self.raw.write(data)
    self.pos += ret
    return ret

  def readline(self) -> bytes:
    assert not self.closed
    ret = b''
    while True:
      buf, found = self.read_buffer.read_until(self.newline)
      ret += buf
      if found:
        break
      buf = self.raw.read(self.chunk_size)
      if not buf:
        break
      self.read_buffer.write(buf)
    self.pos += len(ret)
    return ret

  def flush(self):
    assert not self.closed
    self.raw.flush()

  def tell(self) -> int:
    return self.pos

  def truncate(self, length: int = None):
    self.raw.truncate(self.pos if length is None else length)

  def close(self):
    assert not self.closed
    self.closed = True
    self.raw.close()

  def __enter__(self):
    return self
  def __exit__(self, *args):
    self.close()

  def __iter__(self):
    assert not self.closed
    while True:
      line = self.readline() 
      if not line:
        break
      yield line

class BufferedIO(BufferedBinaryIO):
  def __init__(self, raw: RawBinaryIO, chunk_size = 4096, newline = '\n', encoding = 'utf-8') -> None:
    super().__init__(raw, chunk_size=chunk_size, newline=newline.encode(encoding) if type(newline) == str else newline)
    self.encoding = encoding
  
  def read(self, amnt = -1) -> str:
    return super().read(amnt).decode(self.encoding)

  def write(self, data: str) -> int:
    return super().write(data.encode(self.encoding))

  def readline(self) -> bytes:
    return super().readline().decode(self.encoding)



class AsyncRawBinaryIO:
  async def seek(self, amnt: int, whence: int = 0):
    raise NotImplementedError()
  async def read(self, amnt = -1) -> bytes:
    raise NotImplementedError()
  async def write(self, data: bytes) -> int:
    raise NotImplementedError()
  async def flush(self):
    pass
  async def tell(self) -> int:
    raise NotImplementedError()
  async def truncate(self, length: int = None):
    raise NotImplementedError()
  async def close(self):
    raise NotImplementedError()

class AsyncBufferedBinaryIO:
  def __init__(self, raw: AsyncRawBinaryIO, chunk_size = 4096, newline = b'\n') -> None:
    self.raw = raw
    self.pos = 0
    self.chunk_size = chunk_size
    self.read_buffer = Buffer()
    self.newline = newline
    self.closed = False

  async def seek(self, amnt: int, whence: int = 0):
    assert not self.closed
    await self.raw.seek(amnt, whence)
    if whence == 0: self.pos = amnt
    elif whence == 1: self.pos += amnt
    elif whence == 2: self.pos = -amnt
    self.read_buffer.trim(self.pos)

  async def read(self, amnt = -1) -> bytes:
    assert not self.closed
    if amnt == 0:
      return b''
    ret = self.read_buffer.read(amnt)
    if amnt == -1:
      ret += await self.raw.read(-1)
    elif len(ret) < amnt:
      ret += await self.raw.read(amnt - len(ret))
    self.pos += len(ret)
    return ret

  async def write(self, data: bytes) -> int:
    assert not self.closed
    ret = await self.raw.write(data)
    self.pos += ret
    return ret

  async def readline(self) -> bytes:
    assert not self.closed
    ret = b''
    while True:
      buf, found = self.read_buffer.read_until(self.newline)
      ret += buf
      if found:
        break
      buf = await self.raw.read(self.chunk_size)
      if not buf:
        break
      self.read_buffer.write(buf)
    self.pos += len(ret)
    return ret

  async def flush(self):
    assert not self.closed
    await self.raw.flush()

  async def tell(self) -> int:
    return self.pos

  async def truncate(self, length: int = None):
    await self.raw.truncate(self.pos if length is None else length)

  async def close(self):
    assert not self.closed
    self.closed = True
    await self.raw.close()

  async def __aenter__(self):
    return self
  async def __aexit__(self, *args):
    await self.close()

  async def __aiter__(self):
    assert not self.closed
    while True:
      line = await self.readline() 
      if not line:
        break
      yield line

class AsyncBufferedIO(AsyncBufferedBinaryIO):
  def __init__(self, raw: AsyncRawBinaryIO, chunk_size = 4096, newline = '\n', encoding = 'utf-8') -> None:
    super().__init__(raw, chunk_size=chunk_size, newline=newline.encode(encoding) if type(newline) == str else newline)
    self.encoding = encoding
  
  async def read(self, amnt = -1) -> str:
    return (await super().read(amnt)).decode(self.encoding)

  async def write(self, data: str) -> int:
    return await super().write(data.encode(self.encoding))

  async def readline(self) -> bytes:
    return (await super().readline()).decode(self.encoding)
