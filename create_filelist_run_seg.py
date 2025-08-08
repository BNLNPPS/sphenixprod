#!/usr/bin/env python3

"""
This script doesn't need to exist anymore. 
It's a strict subset of create_full_filelist_run_seg
"""

import create_full_filelist_run_seg
from simpleLogger import slogger, WARN

if __name__ == "__main__":
    slogger.setLevel("DEBUG")
    WARN("Deprecated. Use create_full_filelist_run_seg.py instead.")
    create_full_filelist_run_seg.main()

