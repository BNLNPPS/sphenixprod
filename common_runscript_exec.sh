#!/usr/bin/env bash
## Run full_command under /usr/bin/time, capturing exit code and resource metrics.
## Source this after setting full_command, e.g.:
##   full_command="root.exe -q -b '...'"
##   . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_exec.sh
##
## Sets: status_f4a, user_cpu, sys_cpu, max_rss_kb
## Follow up with stageout calls as needed, then source common_runscript_finish.sh.

echo "--- Executing macro"
echo "${full_command}"
time_file=$(mktemp)
/usr/bin/time -v -o "${time_file}" bash -c "${full_command}"
status_f4a=$?
user_cpu=$(  awk '/User time \(seconds\)/     {print $NF}' "${time_file}")
sys_cpu=$(   awk '/System time \(seconds\)/   {print $NF}' "${time_file}")
max_rss_kb=$(awk '/Maximum resident set size/ {print $NF}' "${time_file}")
rm -f "${time_file}"

if [[ ${status_f4a} -ne 0 ]]; then
    echo "ERROR: Macro exited with code ${status_f4a}. Aborting."
    . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
fi
