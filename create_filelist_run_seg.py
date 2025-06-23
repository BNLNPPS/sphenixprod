#!/usr/bin/env python3

import sys
from collections import defaultdict

from sphenixdbutils import cnxn_string_map, dbQuery # type: ignore

def main():
    if len(sys.argv) != 4:
        script_name = sys.argv[0]
        print(f"usage: {script_name} <inbase> <runnumber> <segment>")
        sys.exit(0)

    inbase = sys.argv[1]
    runnumber_str = sys.argv[2]
    segment_str = sys.argv[3]
    
    try:
        runnumber = int(runnumber_str)
        segment = int(segment_str)
    except ValueError:
        print(f"Error: runnumber '{runnumber_str}' must be an integer.")
        print(f"     : segment '{segment_str}' must be an integer.")
        sys.exit(1)

    # Using a defaultdict to easily append to lists of filenames per host
    #file_list_by_host = defaultdict(list)

    # Note: dsttype isn't actually needed
    sql_query = f"""
    SELECT filename, dsttype 
    FROM datasets 
    WHERE filename like '{inbase}%'
      AND runnumber = {runnumber}
      AND segment ={segment}
    ORDER BY filename
    """
    rows = dbQuery( cnxn_string_map['fcr'], sql_query).fetchall()
    file_list=[]
    for row in rows:
        filename, dsttype = row
        # file_list_by_host[dsttype].append(filename)
        file_list.append(filename)

    if not file_list:
        print("No files found for the given criteria.")

    list_filename = "infile.list"
    try:
        with open(list_filename, 'w') as f_out:
            for fname in file_list:
                f_out.write(f"{fname}\n")
    except IOError as e:
            print(f"Error writing to file {list_filename}: {e}")

if __name__ == "__main__":
    main()
