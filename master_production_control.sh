#!/bin/bash

# This script is intended to be called from a single cron job.
# It sets up the environment and then calls a Python script
# to dispatch production control jobs for various steer files.

# Get the directory of this script to robustly locate other scripts
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Source the main environment setup. Redirect output as it's noisy.
source "${SCRIPT_DIR}/this_sphenixprod.sh" >& /dev/null

if [ -z "$1" ]; then
    echo "Usage: $0 <path_to_steer_list_file> [stagger_seconds]" >&2
    echo "Error: First argument (path to steer list file) is required." >&2
    exit 1
fi

# The first argument is the file listing active production steer files.
ACTIVE_PROD_LIST=$1
# The second argument is the stagger time in seconds. Default to 120 if not provided.
STAGGER_SECONDS=${2:-180}

# Call the dispatcher script.
# The dispatcher will read the list of steer files and run production_control.py for each.
# Stagger launches to avoid all jobs starting at the same time.
"${SCRIPT_DIR}/dispatch_productions.py" --steer-list "${ACTIVE_PROD_LIST}" --stagger "${STAGGER_SECONDS}" -vv
