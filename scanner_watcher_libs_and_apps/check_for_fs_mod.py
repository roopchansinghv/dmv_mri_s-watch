#!/usr/bin/env python



import   asyncio
import   sys
# import   subprocess
# import   datetime

from     pathlib                    import Path

from     watchdog.events            import FileSystemEvent, FileSystemEventHandler
# from     watchdog.observers         import Observer
from     watchdog.observers.polling import PollingObserver



class _EventHandler(FileSystemEventHandler):

   def __init__(self, queue: asyncio.Queue, loop: asyncio.BaseEventLoop,
                observer: watchdog.observers.polling.PollingObserver,
                *args, **kwargs):

      self._loop      = loop
      self._queue     = queue
      self._observer  = observer
      super(*args, **kwargs)

   # remove all separate def - filesystem events, i.e. "on_modified",
   # "on_deleted", "on_created", "on_moved" - as all we are concerned
   # with catching are *ANY* log changes, except for "on_deleted" -
   # which might be a little harder to deal with ... ;-)

   def on_any_event(self, event: FileSystemEvent) -> None:

      self._loop.call_soon_threadsafe(self._queue.put_nowait, event)

      print(event.event_type, event.src_path)

      if ((event.event_type == "modified") or (event.event_type == "created") or (event.event_type == "moved")):
         print ("Path: %50s has event: %s" % (event.src_path, event.event_type))

      if (event.event_type == "created"):
         self._observer.stop()
         return (event.src_path)



async def watch(path: Path, queue: asyncio.Queue, loop: asyncio.BaseEventLoop,
          recursive: bool = False) -> None:

   """Watch a file or directory for changes."""

   # observer = Observer()
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

   try:
      asyncio.run(watch(Path(watched_path), queue, loop, True))
   except KeyboardInterrupt:
      loop.close()
      print("Exiting ...")
   finally:
      sys.exit(0)

