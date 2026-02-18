#!/usr/bin/env bash

if [ "$#" -ne 1 ] && [ "$#" -ne 4 ] ; then
    echo "Unsupported call:"
    echo $0 $@
    echo Abort.
    exit 10
fi

distfilename=${1}
echo "Checking health of file ${distfilename}"
if [ "$#" -eq 1 ] ; then
    echo "No md5 or size check requested, assume success"
    exit 0
fi

md5=-1
size=-1
filesystem=""
if [ "$#" -eq 4 ] ; then
    md5=${2}
    size=${3}
    filesystem=${4}
    # [ "${filesystem}" != "sphenix" ] && [ "${filesystem}" != "gpfs" ] && 
    if [ "${filesystem}" != "lustre" ] ; then
        echo "Unsupported filesystem ${filesystem} (expect lustre). Abort."
        exit 10
    fi
fi

if [ ! -f ${distfilename} ]; then
    echo "${distfilename} not found!"
    exit 11
fi

# Check size
if [ "${size}" != "-1" ] ; then
    actual_size=`stat -c '%s' ${distfilename}`
    if [ "${actual_size}" == "${size}" ] ; then
        echo "Size check passed."
    else
        echo "Calculated size: ${actual_size}"
        echo "Expected size: ${size}"
        exit 1
    fi
else
    # No size check requested, assume success
    echo "No size check requested, assume success"
    break
fi

# Check md5sum:
actual_md5=`/usr/bin/env md5sum ${distfilename} | cut -d ' ' -f 1`

if [ "${md5}" != "-1" ] ; then
    if [ "${actual_md5}" != "${md5}" ] ; then
        echo "Calculated md5: ${actual_md5}"
        echo "Expected md5  : ${md5}"
        echo "md5sum mismatch! Abort."
        exit 1
    fi
    echo "Md5sum check passed."
else 
    echo "No md5sum check requested, assume success"
fi

exit 0
