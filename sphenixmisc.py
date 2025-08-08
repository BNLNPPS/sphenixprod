from pathlib import Path
from typing import Set,List
from datetime import datetime
from logging.handlers import RotatingFileHandler
import subprocess
import bisect # for binary search in sorted lists

from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixdbutils import test_mode as dbutils_test_mode

# ============================================================================================
def shell_command(command: str) -> List[str]:
    """Minimal wrapper to hide away subbprocess tedium"""
    CHATTY(f"[shell_command] Command: {command}")
    ret=[]
    try:
        ret = subprocess.run(command, shell=True, check=True, capture_output=True).stdout.decode('utf-8').split()
    except subprocess.CalledProcessError as e:
        WARN("[shell_command] Command failed with exit code:", e.returncode)
    finally:
        pass

    CHATTY(f"[shell_command] Return value length is {len(ret)}.")
    return ret

# ============================================================================================
def setup_rot_handler(args):
    #################### Test mode?
    test_mode = (
        dbutils_test_mode
        or args.test_mode
    )
    if not args.sublogdir:
        if test_mode:
            sublogdir='/tmp/testbed/sphenixprod/'
        else:
            sublogdir='/tmp/sphenixprod/sphenixprod/'
    sublogdir += f"{args.rulename}".replace('.yaml','')
    Path(sublogdir).mkdir( parents=True, exist_ok=True )
    RotFileHandler = RotatingFileHandler(
        filename=f"{sublogdir}/{str(datetime.today().date())}.log",
        mode='a',
        maxBytes=25*1024*1024, #   maxBytes=5*1024,
        backupCount=10,
        encoding=None,
        delay=0
    )
    RotFileHandler.setFormatter(CustomFormatter())
    slogger.addHandler(RotFileHandler)

    return sublogdir

# ============================================================================================
def should_I_quit(args, myname) -> bool:
    # Exit without fuss if we are already running
    p = subprocess.Popen("ps axuww | /usr/bin/grep $USER",shell=True,stdout=subprocess.PIPE)
    stdout_bytes, stderr_bytes = p.communicate() # communicate() returns bytes
    stdout_str = stdout_bytes.decode(errors='ignore') # Decode to string

    # Construct a search key with script name, config file, and rulename
    # to check for other running instances with the same parameters.
    count_already_running = 0
    for psline in stdout_str.splitlines():
        if myname in psline and args.config in psline and args.rulename in psline:
            count_already_running += 1

    CHATTY ( f"Found {count_already_running} instance(s) of {myname} with config {args.config} and rulename {args.rulename} in the process list.")
    if count_already_running == 0:
        ERROR("No running instance found, including myself. That can't be right.")
        exit(1)

    if count_already_running > 1:
        DEBUG(f"Looks like there's already {count_already_running-1} running instance(s) of me. Suggest Stop.")
        return True

    return False

# ============================================================================================
def make_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    # source https://stackoverflow.com/questions/312443/how-do-i-split-a-list-into-equally-sized-chunks
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# ============================================================================================
def binary_contains_bisect(arr, x):
    pos = bisect.bisect_left(arr, x)
    if pos != len(arr) and arr[pos] == x:
        return True # pos
    return False # -1

# ============================================================================================
def remove_empty_directories(dirs_to_del: Set[str]):
    """
    Recursively removes all empty subdirectories within the given set of directories.
    If directory_path itself becomes empty after its subdirectories are processed,
    it will also be removed.

    Args:
        dirs_to_del (Set): The directories to process. Used to pop() and insert()
    """
    while dirs_to_del:
        dir = Path(dirs_to_del.pop())
        CHATTY(f"Called for {dir}")
        if not dir.is_dir():
            continue
        # In principle, don't need the not any iter call here, the directory dhoiuld be empty by definition
        if not any(dir.iterdir()):
            try:
                dir.rmdir()
            except OSError as e:
                # This might occur due to permission issues, or if the directory is
                # unexpectedly not empty (e.g., hidden files not listed by iterdir
                # on some specific OS/filesystem configurations, or a race condition).
                print(f"Warning: Could not remove directory '{dir}'. Reason: {e}")
                continue
        parent=dir.parent
        # Check the parent, if empty, add to the set
        if not any(parent.iterdir()):
            dirs_to_del.add(str(parent))
