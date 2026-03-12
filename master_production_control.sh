#!/bin/bash

# This script is intended to be called from a single cron job.
# It sets up the environment and then calls a Python script
# to dispatch production control jobs for various steer files.

# Get the directory of this script to robustly locate other scripts
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Source the main environment setup. Redirect output as it's noisy.
source "${SCRIPT_DIR}/this_sphenixprod.sh" >& /dev/null

# The file listing active production steer files
ACTIVE_PROD_LIST="${SCRIPT_DIR}/../ProdFlow/short/active_productions.txt"

# Call the dispatcher script.
# The dispatcher will read the list of steer files and run production_control.py for each.
# Stagger launches to avoid all jobs starting at the same time.

# Use the first command-line argument as the stagger time in seconds.
# If no argument is provided, default to 120 seconds.
STAGGER_SECONDS=${1:-120}

"${SCRIPT_DIR}/dispatch_productions.py" --steer-list "${ACTIVE_PROD_LIST}" --stagger "${STAGGER_SECONDS}" -vv
