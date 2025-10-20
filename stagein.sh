#!/usr/bin/env bash

if [ "$#" -ne 1 ] && [ "$#" -ne 4 ] ; then
    echo "Unsupported call:"
    echo $0 $@
    echo Abort.
    exit 0
fi

distfilename=${1}
echo "Staging in file ${distfilename}"

md5=-1
size=-1
filesystem=""
if [ "$#" -eq 4 ] ; then
    md5=${2}
    size=${3}
    filesystem=${4}
    echo "Expected md5: ${md5}"
    echo "Expected size: ${size}"
    # [ "${filesystem}" != "sphenix" ] && [ "${filesystem}" != "gpfs" ] && 
    if [ "${filesystem}" != "lustre" ] ; then
        echo "Unsupported filesystem ${filesystem} (expect lustre). Abort."
        exit 1
    fi
fi

if [ ! -f ${distfilename} ]; then
    echo "${distfilename} not found!"
    exit 1
fi

filename=`basename ${distfilename}` # Strips the path
action="dd if=${distfilename} of=./${filename} bs=12MB"
echo ${action}
eval ${action}

# # Check md5sum:
# actual_md5=`/usr/bin/env md5sum ${filename} | cut -d ' ' -f 1`
# if [ "$#" -eq 4 ] ; then
#     if [ "${actual_md5}" != "${md5}" ] ; then
#         echo "Calculated md5: ${actual_md5}"
#         echo "Expected md5: ${md5}"
#         echo "md5sum mismatch! Abort."
#         exit 1
#     fi
# fi

# Check size
actual_size=`stat -c '%s' ${filename}`
if [ "$#" -eq 4 ] ; then
    if [ "${actual_size}" != "${size}" ] ; then
        echo "Calculated size: ${actual_size}"
        echo "Expected size: ${size}"
        echo "Size mismatch! Abort."
        exit 1
    fi
fi


exit 0
