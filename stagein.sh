#!/usr/bin/env bash

# --checkonly: validate that the preceding filelist creator succeeded.
# Must be sourced (. stagein.sh --checkonly) so it can exit the calling wrapper.
if [[ "${1}" == "--checkonly" ]]; then
    _filelist_rc=$?  # exit status of the filelist creator — preserved by bash into sourced scripts
    if [[ $_filelist_rc -ne 0 ]]; then
        echo "ERROR: Filelist creation failed (exit code $_filelist_rc). Aborting job."
        status_f4a=$_filelist_rc
        . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
    fi
    echo "Filelist creation succeeded."
    shopt -s nullglob
    for l in *.list; do
        echo "--- $l"
        ls -la "$l"
        cat "$l"
    done
    shopt -u nullglob

    if [[ -s infile_paths.list ]]; then
        echo "--- Checking remote file health from infile_paths.list"
        _health_ok=1
        while IFS=' ' read -r full_file_path md5 size full_host_name; do
            actual_size=$(stat -c '%s' "${full_file_path}" 2>/dev/null)
            if [[ -z "${actual_size}" ]]; then
                echo "MISSING: ${full_file_path}"
                _health_ok=0
            elif [[ "${actual_size}" != "${size}" ]]; then
                echo "SIZE MISMATCH: ${full_file_path} (expected ${size}, got ${actual_size})"
                _health_ok=0
            else
                echo "OK: ${full_file_path} (${size} bytes)"
            fi
        done < infile_paths.list
        if [[ ${_health_ok} -eq 0 ]]; then
            echo "ERROR: Remote file health check failed. Aborting job."
            status_f4a=11
            . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
        fi
        echo "Remote file health check passed."
    fi

    return 0 2>/dev/null
fi

# Staging mode: read infile_paths.list and dd each file into the working directory.
# Must be sourced so it can exit the calling wrapper on failure.
# infile_paths.list format (from create_full_filelist_run_seg.py):
#   full_file_path md5 size full_host_name
infile_paths="infile_paths.list"
if [[ ! -s "${infile_paths}" ]]; then
    echo "ERROR: ${infile_paths} not found or empty. Cannot stage in files."
    status_f4a=20
    . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
fi

maxtries=2
while IFS=' ' read -r full_file_path md5 size full_host_name; do
    filename=$(basename "${full_file_path}")
    echo "Staging in: ${full_file_path} -> ./${filename} (size=${size}, md5=${md5})"

    if [[ ! -f "${full_file_path}" ]]; then
        echo "ERROR: Source file ${full_file_path} not found. Aborting."
        status_f4a=20
        . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
    fi

    for try in $(seq 1 ${maxtries}); do
        [[ ${try} -gt 1 ]] && echo "Attempt ${try}/${maxtries}"
        dd if="${full_file_path}" of="./${filename}" bs=12MB 2>&1 | awk '
            /records in|records out/ { next }
            /copied/ { print; next }
            { print > "/dev/stderr" }
        '
        actual_size=$(stat -c '%s' "./${filename}" 2>/dev/null)
        if [[ "${actual_size}" == "${size}" ]]; then
            echo "Size check passed."
            break
        fi
        echo "Size mismatch on attempt ${try}/${maxtries} (expected ${size}, got ${actual_size:-<missing>})."
        rm -f "./${filename}"
        if [[ ${try} -eq ${maxtries} ]]; then
            echo "ERROR: All ${maxtries} attempts failed for ${full_file_path}. Aborting."
            status_f4a=20
            . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
        fi
    done

    # Once size is OK, md5 has to be too — mismatch indicates a deeper problem.
    actual_md5=$(/usr/bin/env md5sum "./${filename}" | cut -d ' ' -f 1)
    if [[ "${actual_md5}" != "${md5}" ]]; then
        echo "ERROR: md5 mismatch for ${filename} (expected ${md5}, got ${actual_md5}). Aborting."
        status_f4a=21
        . ${SPHENIXPROD_SCRIPT_PATH}/common_runscript_finish.sh
    fi
    echo "md5 check passed."

done < "${infile_paths}"

return 0
