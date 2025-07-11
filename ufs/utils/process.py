''' This is a helper for running a process in the background and ensuring
1. if it exits before the context manager is complete, an exception is raised
2. when the context manager is complete, the process is killed

This is achieved with a helper thread, it works as follows:
  active_process called
  process_thread created with a shared queue
  the instantiated process is passed to the thread over the queue to be managed
  the thread starts the process and waits for it to exit
  if an exception occurs or the process exits, an exception is put on the queue iff the queue is empty, and main is interrupted with KeyboardInterrupt
  on the main thread, we capture the KeyboardInterrupt, take the exception off the queue and raise it
  if the context manager completes, we add to the queue to make it non-empty before terminating the process which will make the thread exit gracefully.
'''
import os
import signal
import contextlib
import threading
import typing as t
import multiprocessing as mp
from subprocess import Popen
from queue import Queue, Empty

mp_spawn = mp.get_context('spawn')

class ProcessExitException(Exception):
  def __init__(self, exitcode) -> None:
    super().__init__(f"Process exited with code {exitcode}")
    self.exitcode = exitcode

def process_thread(queue: Queue):
  proc: t.Union[mp.Process, mp.Process, Popen] = queue.get()
  exception = None
  try:
    if isinstance(proc, mp.Process) or isinstance(proc, mp_spawn.Process):
      proc.start()
      proc.join()
      exception = ProcessExitException(proc.exitcode)
    elif isinstance(proc, Popen):
      proc.wait()
      exception = ProcessExitException(proc.returncode)
    else:
      exception = NotImplementedError(type(proc))
  except Exception as exc:
    exception = exc
  queue.task_done()
  try:
    queue.get_nowait()
    queue.task_done()
  except Empty:
    queue.put(exception)
    current_pid = mp.current_process().pid
    if current_pid:
      os.kill(current_pid, signal.SIGINT)

@contextlib.contextmanager
def active_process(proc: t.Union[mp.Process, mp_spawn.Process, Popen], *, terminate_signal=signal.SIGTERM):
  queue = Queue()
  thread = threading.Thread(
    target=process_thread,
    args=(queue,),
  )
  thread.start()
  queue.put(proc)
  try:
    yield
  except KeyboardInterrupt:
    try:
      exc = queue.get_nowait()
      queue.task_done()
      raise exc
    except Empty:
      raise KeyboardInterrupt
  finally:
    queue.put(None)
    if isinstance(proc, mp.Process) or isinstance(proc, mp_spawn.Process):
      if proc.is_alive():
        proc_pid = proc.pid
        if proc_pid:
          os.kill(proc_pid, terminate_signal)
        proc.join()
    elif isinstance(proc, Popen):
      if proc.poll() is None:
        os.kill(proc.pid, terminate_signal)
        proc.wait()
    else:
      raise NotImplementedError(type(proc))
    thread.join()
