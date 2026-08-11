#!/usr/bin/env python



import   asyncio
import   sys

from     pathlib                    import Path

from     watchdog.events            import FileSystemEvent, FileSystemEventHandler
from     watchdog.observers.polling import PollingObserver



global   file_written

class _EventHandler(FileSystemEventHandler):

   def __init__(self, queue: asyncio.Queue, loop: asyncio.BaseEventLoop,
                observer: PollingObserver, *args, **kwargs):

      self._loop      = loop
      self._queue     = queue
      self._observer  = observer
      super(*args, **kwargs)

   def on_event(self, event: FileSystemEvent) -> None:

      self._loop.call_soon_threadsafe(self._queue.put_nowait, event)

      print ("Path: %50s has event: %s" % (event.src_path, event.event_type))

   def on_modified(self, event: FileSystemEvent) -> None:

      self.on_event(event)

   def on_deleted(self, event: FileSystemEvent) -> None:

      self.on_event(event)

   def on_moved(self, event: FileSystemEvent) -> None:

      self.on_event(event)

   def on_created(self, event: FileSystemEvent) -> None:

      self.on_event(event)

      if (event.event_type == "created"):
         path_object = Path(event.src_path)
         if Path.is_file(path_object):
            global file_written
            file_written = str(event.src_path)
            self._observer.stop()
         elif Path.is_dir(path_object):
            print(f"Directory {event.src_path} created, continue watching")
         else:
            print(f"Unknown entity {event.src_path} created, continue watching")



async def watch(path: Path, queue: asyncio.Queue, loop: asyncio.BaseEventLoop,
          recursive: bool = False) -> None:

   """Watch a file or directory for changes."""

   observer = PollingObserver()

   handler = _EventHandler(queue, loop, observer)

   observer.schedule(handler, str(path), recursive=recursive)
   observer.start()
   print("Observer started")
   observer.join(None) # Remove value or set to None, to allow to run indefinitely
   loop.call_soon_threadsafe(queue.put_nowait, None)



if __name__ == "__main__":

   if len(sys.argv[1:]) > 0:
      watched_path = sys.argv[1]
   else:
      print("Please specify directory or file to observe")
      sys.exit(-1)

   loop   = asyncio.new_event_loop()
   queue  = asyncio.Queue()

   global file_written
   file_written = ''

   try:
      asyncio.run(watch(Path(watched_path), queue, loop, True))
   except KeyboardInterrupt:
      loop.close()
      print("Exiting ...")
   finally:
      print(f"File written is: {file_written}")
      sys.exit(0)

