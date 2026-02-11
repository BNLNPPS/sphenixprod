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

filename=`basename ${distfilename}` # Strips the path

maxtries=3
for i in `seq 1 $maxtries`; do
    action="dd status=none if=${distfilename} of=./${filename} bs=12MB"
    if [ $i -gt 1 ] ; then
        echo "Attempt $i: ${action}"
    fi
    eval ${action}

    # Check size
    if [ "${size}" != "-1" ] ; then
        actual_size=`stat -c '%s' ${filename}`
        if [ "${actual_size}" == "${size}" ] ; then
            echo "Size check passed."
            break # Exit loop
        else
            echo "Calculated size: ${actual_size}"
            echo "Expected size: ${size}"
            echo "Size mismatch on attempt $i."
            if [ $i -eq $maxtries ]; then
                rm ${filename} # clean up incomplete file
                echo "Size mismatch after $maxtries attempts. Abort."
                exit 12
            fi
            # Try again
        fi
    else
        # No size check requested, assume success
        echo "No size check requested, assume success"
        break
    fi
done
# Once the size is okay, md5sum has to be too, otherwise the problem is more profound than transfer issues.

# Check md5sum:
actual_md5=`/usr/bin/env md5sum ${filename} | cut -d ' ' -f 1`

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
