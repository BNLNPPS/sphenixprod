#!/usr/bin/env bash
## Run full_command under /usr/bin/time, capturing exit code and resource metrics.
## Source this after setting full_command, e.g.:
##   full_command="root.exe -q -b '...'"
##   . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_exec.sh
##
## Sets: status_f4a, user_cpu, sys_cpu, exec_wall_sec, max_rss_kb, exec_cpu_percent
## Follow up with stageout calls as needed, then source common_runscript_finish.sh.

echo "--- Executing macro"
echo "${full_command}"
time_file=$(mktemp)
/usr/bin/time -f "user_cpu=%U
sys_cpu=%S
exec_wall_sec=%e
max_rss_kb=%M
exec_cpu_percent=%P" -o "${time_file}" bash -c "${full_command}"
status_f4a=$?
user_cpu=$(awk -F= '$1=="user_cpu" {print $2}' "${time_file}")
sys_cpu=$(awk -F= '$1=="sys_cpu" {print $2}' "${time_file}")
exec_wall_sec=$(awk -F= '$1=="exec_wall_sec" {print $2}' "${time_file}")
max_rss_kb=$(awk -F= '$1=="max_rss_kb" {print $2}' "${time_file}")
exec_cpu_percent=$(awk -F= '$1=="exec_cpu_percent" {gsub(/%/, "", $2); print $2}' "${time_file}")
rm -f "${time_file}"

if [[ ${status_f4a} -ne 0 ]]; then
    echo "ERROR: Macro exited with code ${status_f4a}. Aborting."
    . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
fi
