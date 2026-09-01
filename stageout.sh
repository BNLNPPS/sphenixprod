#!/usr/bin/env bash

stageout_sourced=0
if [ "${BASH_SOURCE[0]}" != "$0" ]; then
    stageout_sourced=1
fi

MIN_ARG_COUNT=2
MAX_ARG_COUNT=3
stageout_copy_command=dd
stageout_args=()
for arg in "$@"; do
    case "${arg}" in
        --use-cp|--cp)
            stageout_copy_command=cp
            ;;
        --use-dd|--dd)
            stageout_copy_command=dd
            ;;
        *)
            stageout_args+=("${arg}")
            ;;
    esac
done
set -- "${stageout_args[@]}"

if [ "$#" -lt "$MIN_ARG_COUNT" ] || [ "$#" -gt "$MAX_ARG_COUNT" ] ; then
    echo "Unsupported call:"
    echo "$0 $*"
    echo "Supported copy flags: --use-dd (default), --use-cp"
    echo Abort.
    status_f4a=2
    . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
fi

stageout_wall_sec=${stageout_wall_sec:-0}
_stageout_start=$(date +%s.%N)

filename=${1}
destination=${2}
dbid=${3:--1} # dbid for faster db lookup, -1 means no dbid

finish_stageout_error() {
    status_f4a=$1
    shift
    echo "ERROR: $*"
    . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
}

require_integer_metadata() {
    local name=$1
    local value=$2
    if [[ ! "${value}" =~ ^-?[0-9]+$ ]]; then
        finish_stageout_error 31 "Invalid ${name} metadata for ${filename}: ${value:-<empty>}"
    fi
}

if [ $dbid -eq -1 ] || [ $dbid -eq 0 ]; then
    # I don't quite understand why dbid can be 0, need to dig around --> later
    # Fallback (or rather default): Use environment variable exported by wrapper
    dbid=${PRODDB_DBID:--1}
fi

if [ ! -f "${filename}" ]; then
    echo "${filename} not found!"
    echo ls -lahtr
    ls -lahtr
    status_f4a=30
    . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
fi

# Could test the destination like this, but we want to minimize lustre probing in the worker jobs
# if [ -d "$destination" ]; then   echo "$DIRECTORY does exist." ; else exit 1; fi

# Number of events and first/last event numbers
rm -f numbers.txt  cleannumbers.txt
root.exe -l -b -q GetEntriesAndEventNr.C\(\"${filename}\"\) > numbers.txt
tail -n 3  numbers.txt  | awk '{print $NF}' > cleannumbers.txt

nevents=`sed -n '1p' cleannumbers.txt`
first=`sed -n '2p' cleannumbers.txt`
last=`sed -n '3p' cleannumbers.txt`

# Set to -1 if empty or 0
nevents=${nevents:--1}
first=${first:--1}
last=${last:--1}

echo cat numbers.txt
cat numbers.txt
echo cat cleannumbers.txt
cat cleannumbers.txt
rm -f numbers.txt cleannumbers.txt

# md5sum:
md5=`/usr/bin/env md5sum "${filename}" | cut -d ' ' -f 1`
if [ -z "${md5}" ]; then
    finish_stageout_error 31 "Could not compute md5 for ${filename}"
fi

# size and ctime
#stat -c '%s %Y'
size_error=$(mktemp "${_CONDOR_SCRATCH_DIR:-/tmp}/sphenixprod_stageout_size.XXXXXX")
ctime_error=$(mktemp "${_CONDOR_SCRATCH_DIR:-/tmp}/sphenixprod_stageout_ctime.XXXXXX")
if ! size=$(stat -c "%s" "${filename}" 2>"${size_error}"); then
    finish_stageout_error 31 "Could not determine file size for ${filename}: $(cat "${size_error}")"
fi
if ! ctime=$(stat -c "%Y" "${filename}" 2>"${ctime_error}"); then
    finish_stageout_error 31 "Could not determine file ctime for ${filename}: $(cat "${ctime_error}")"
fi
rm -f "${size_error}" "${ctime_error}"

require_integer_metadata nevents "${nevents}"
require_integer_metadata first "${first}"
require_integer_metadata last "${last}"
require_integer_metadata size "${size}"
require_integer_metadata ctime "${ctime}"

#change the destination filename
destname=`basename "${filename}"`
destname="${destname}:nevents:${nevents}"
destname="${destname}:first:${first}"
destname="${destname}:last:${last}"
destname="${destname}:md5:${md5}"
destname="${destname}:size:${size}"
destname="${destname}:ctime:${ctime}"
destname="${destname}:dbid:${dbid}"

# Track peak scratch disk usage across all stageout calls.
# $_CONDOR_SCRATCH_DIR is job-local; fall back to /tmp for interactive runs.
diskpeak_file=${_CONDOR_SCRATCH_DIR:-/tmp}/sphenixprod_diskpeak
current_kb=$(du -sk . | awk '{print $1}')
stored_kb=$(cat "${diskpeak_file}" 2>/dev/null || echo 0)
if [ "${current_kb}" -gt "${stored_kb}" ]; then
    echo "${current_kb}" > "${diskpeak_file}"
fi

mkdir -p "${destination}"

copy_dest="${destination}/${destname}"
max_tries=2

for try in $(seq 1 ${max_tries}); do
    if [ "${stageout_copy_command}" = "cp" ]; then
        echo cp -p "${filename}" "${copy_dest}"
        cp -p "${filename}" "${copy_dest}"
    else
        echo dd if="${filename}" of="${copy_dest}" bs=12MB
        dd if="${filename}" of="${copy_dest}" bs=12MB 2>&1 | awk '
            /records in|records out/ { next }
            /copied/ { print; next }
            { print > "/dev/stderr" }
        '
    fi

    dest_size=$(stat -c '%s' "${copy_dest}" 2>/dev/null)
    if [ "${dest_size}" = "${size}" ]; then
        break
    fi
    echo "Size mismatch on attempt ${try}/${max_tries} (expected ${size}, got ${dest_size:-<missing>})."
    rm -f "${copy_dest}"
    if [ ${try} -eq ${max_tries} ]; then
        echo "ERROR: All ${max_tries} attempts failed. Giving up."
        status_f4a=31
        . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
    fi
done

rm -v "${filename}"

_stageout_end=$(date +%s.%N)
_stageout_dt=$(awk "BEGIN {print ${_stageout_end} - ${_stageout_start}}")
stageout_wall_sec=$(awk "BEGIN {print ${stageout_wall_sec} + ${_stageout_dt}}")

if [ "${stageout_sourced}" -eq 1 ]; then
    return 0
fi
exit 0
