#!/usr/bin/env python

import argparse
import subprocess
from pathlib import Path
import sys
import time

from simpleLogger import slogger, CustomFormatter, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL
from logging.handlers import RotatingFileHandler
from datetime import datetime

def setup_dispatcher_logging(args):
    """Set up a rotating log handler for the dispatcher."""
    logdir = '/tmp/sphenixprod/sphenixprod/dispatcher'
    Path(logdir).mkdir(parents=True, exist_ok=True)
    
    handler = RotatingFileHandler(
        filename=f"{logdir}/{str(datetime.today().date())}.log",
        mode='a',
        maxBytes=5*1024*1024, # 5 MB
        backupCount=5,
        encoding=None,
        delay=0
    )
    handler.setFormatter(CustomFormatter())
    slogger.addHandler(handler)
    slogger.setLevel(args.loglevel)
    INFO("Dispatcher logging setup complete.")

def main():
    parser = argparse.ArgumentParser(description="Dispatch multiple production_control jobs.")
    parser.add_argument('--steer-list', required=True, help="A text file listing the steer files to process, one per line.")
    parser.add_argument('--stagger', type=int, default=60, help="Seconds to wait between dispatching each job.")

    vgroup = parser.add_argument_group('Logging level')
    exclusive_vgroup = vgroup.add_mutually_exclusive_group()
    exclusive_vgroup.add_argument('-v', '--verbose', help="Prints more information per repetition", action='count', default=0)
    exclusive_vgroup.add_argument('-d', '--debug', help="Prints even more information", action="store_true")
    exclusive_vgroup.add_argument('-c', '--chatty', help="Prints the most information", action="store_true")
    exclusive_vgroup.add_argument('--loglevel', dest='loglevel', default='INFO',
                                  help="Specific logging level (CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL)")

    from argparsing import parse_and_set_loglevel
    args = parse_and_set_loglevel(parser)

    setup_dispatcher_logging(args)

    steer_list_file = Path(args.steer_list)
    if not steer_list_file.is_file():
        ERROR(f"Steer list file '{steer_list_file}' not found.")
        sys.exit(2)

    with open(steer_list_file, 'r') as f:
        steer_files = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]

    if not steer_files:
        INFO(f"No active steer files found in '{steer_list_file}'. Exiting.")
        return

    INFO(f"Found {len(steer_files)} steer files to process from {steer_list_file}.")

    for i, steer_file_path in enumerate(steer_files):
        steer_file = Path(steer_file_path)
        if not steer_file.is_file():
            WARN(f"Steer file '{steer_file}' not found, skipping.")
            continue

        command = [ "production_control.py", "--steerfile", str(steer_file), f"--loglevel={args.loglevel}" ]
        
        INFO(f"Dispatching: {' '.join(command)}")
        result = subprocess.run(command)

        if result.returncode == 2:
            DEBUG(f"production_control exited with 2 (host not in steer file), skipping stagger.")
        elif args.stagger > 0 and i < len(steer_files) - 1:
            INFO(f"Waiting for {args.stagger} seconds before next dispatch.")
            time.sleep(args.stagger)

    INFO("All production control jobs dispatched.")

if __name__ == "__main__":
    main()