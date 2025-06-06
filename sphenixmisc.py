from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler
import subprocess

from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixdbutils import test_mode as dbutils_test_mode

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
    p = subprocess.Popen(["ps","axuww"], stdout=subprocess.PIPE)
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
        DEBUG(f"Looks like there's already {count_already_running} running instance(s) of me. Suggest Stop.")
        return True
        
    return False
