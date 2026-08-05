
import   os, sys
import   asyncio
import   socket
import   datetime
import   logging
import   subprocess
import   signal

sys.path.insert(0, os.path.abspath('.'))
import   Shared

from     pathlib                    import Path

from     watchdog.events            import FileSystemEvent, FileSystemEventHandler
from     watchdog.observers.polling import PollingObserver
from     watchdog.observers         import Observer



state_poll_interval = 0.5   # in seconds
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y_%m_%d %H:%M:%S :',
                    level=logging.WARNING)
scan_event_logger   = logging.getLogger(__name__)



# create a few global variables to help with scanner state tracking
global patient_in_scanner, afni_running, pid_afni, data_being_acquired, pid_dimon
global last_data_dir_dicom



async def poll_state(polling_interval, host = '127.0.0.1', port = 5555):

   global last_data_dir_dicom

   while True:

      current_state_dict  = {}

      await asyncio.sleep(polling_interval)

      with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as watched_socket:

         watched_socket.connect ((host, int(port)))

         socket_data = watched_socket.recv(1 * 1024 * 1024)

      # Convert published JSON struct to Python dictionary
      data = eval(socket_data.decode('utf-8'))  # should find a way to use
                                                # ast.literal_eval() to do this
      # Extract desired information from packet
      current_state_dict = data['all_events']

      scan_event_logger.info('Scanner: ' + data['scanner AE Title']
            + " from vendor: " + data['scanner vendor'] + " has events: "
            + str(data['all_events']) + ' detected at '
            + datetime.datetime.now().strftime("%Y_%m_%d_%H:%M:%S") + '\n')

      process_current_state (data)  # Pass along request response as json, and
                                    # process appropriately in calling function.

      # If called here, directory watch task gets called when directory changes
      # in any way, but doesn't work in parallel/asynchronously.  This is still
      # a blocking call ...
      # if (data_being_acquired == True):
         # await watch(Path(last_data_dir_dicom), True)



def process_current_state(state_to_process):

   scanner_events_dict = state_to_process['all_events']

   global patient_in_scanner, afni_running, data_being_acquired, afni_process
   global last_data_dir_dicom

   last_data_dir_dicom = scanner_events_dict['session image data directory']

   if ((scanner_events_dict['End scanning session'] <
        scanner_events_dict['Start scanning session']) and not patient_in_scanner):
      patient_in_scanner = True

   if ((scanner_events_dict['End scanning session'] >
        scanner_events_dict['Start scanning session']) and patient_in_scanner):
      patient_in_scanner = False

   if (patient_in_scanner and not afni_running):
      afni_running = True
      scan_event_logger.info("Should start AFNI now")
      dir_afni     = f'{os.path.join(os.environ['MRI_SCANNER_DATA_DIR_AFNI'],
                                     datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))}'
      os.makedirs(dir_afni, exist_ok=True)
      os.chmod(dir_afni, 0o5777) # When creating directory, allow users to interact with
                                 # data being written there, but not allow to delete them.
      os.chdir(dir_afni)
      afni_process = subprocess.Popen(['afni', '-rt'],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       text=False)

   if (afni_running and not patient_in_scanner):
      afni_running = False
      scan_event_logger.info("Should stop AFNI now")
      # Process ID returned by sub-process seems to be for the shell which
      # spawns the actual AFNI process, so use the following to get PID of
      # the acutal running AFNI real-time process.
      afni_process_stop = subprocess.run("ps -ef | grep 'afni -rt' | grep -v grep | tr -s ' ' | cut -d ' ' -f2 -",
                                         shell=True, text=True,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE)
      os.kill(int(afni_process_stop.stdout), signal.SIGTERM)

   if (afni_running    and     (scanner_events_dict['Pulse sequence prepped'] >
                                scanner_events_dict['Scanner is done acquiring data'])):
      data_being_acquired = True

   if (data_being_acquired and (scanner_events_dict['Pulse sequence prepped'] <
                                scanner_events_dict['Scanner is done acquiring data'])):
      data_being_acquired = False

   scan_event_logger.warning(f"patient_in_scanner = {patient_in_scanner}, afni_running = {afni_running}, data_being_acquired = {data_being_acquired}, current_data_dir_dicom = {last_data_dir_dicom}")

   return



class _EventHandler(FileSystemEventHandler):

   def __init__(self, *args, **kwargs):

      super(*args, **kwargs)

   # remove all separate def - filesystem events, i.e. "on_modified",
   # "on_deleted", "on_created", "on_moved" - as all we are concerned
   # with catching are *ANY* log changes, except for "on_deleted" -
   # which might be a little harder to deal with ... ;-)

   def on_any_event(self, event: FileSystemEvent) -> None:

      print(event.event_type, event.src_path)

      if ((event.event_type == "modified") or (event.event_type == "created") or (event.event_type == "moved")):
         print ("Path: %50s has event: %s" % (event.src_path, event.event_type))



async def watch(path: Path, recursive: bool = False) -> None:

   """Watch a file or directory for changes."""

   handler = _EventHandler()

   observer = PollingObserver()
   observer.schedule(handler, str(path), recursive=recursive)
   observer.start()
   print("Observer started")
   observer.join(None) # Remove value or set to None, to allow to run indefinitely



async def gather_and_run_client_tasks():

   # Check for all needed environment variables first!

   environment_vars = ['MRI_SCANNER_INFO_PUBLISH_TO_HOST',
                       'MRI_SCANNER_INFO_PUBLISH_TO_PORT',
                       'MRI_SCANNER_DATA_DIR_DICOM',
                       'MRI_SCANNER_DATA_DIR_AFNI']

   Shared.routines.check_env_vars(environment_vars)

   # If all necessary environment variables have been defined, proceed with program
   # execution.

   global patient_in_scanner
   patient_in_scanner = False
   global afni_running
   afni_running = False
   global data_being_acquired
   data_being_acquired = False
   global last_data_dir_dicom
   last_data_dir_dicom = os.environ['MRI_SCANNER_DATA_DIR_DICOM']

   client_tasks = []

   task_polling_state = asyncio.create_task(poll_state(state_poll_interval,
                                            host=os.environ['MRI_SCANNER_INFO_PUBLISH_TO_HOST'],
                                            port=os.environ['MRI_SCANNER_INFO_PUBLISH_TO_PORT']))
   client_tasks.append(task_polling_state)

   # If implemented here, task queue is blocked
   # task_watch_dicom_dir = asyncio.create_task(watch(Path(last_data_dir_dicom), True))
   # client_tasks.append(task_watch_dicom_dir)

   await asyncio.gather(*client_tasks)



if __name__ == "__main__":

   try:
      asyncio.run(gather_and_run_client_tasks())

   except KeyboardInterrupt:
      print("Stopping task.")
      exit(0)

