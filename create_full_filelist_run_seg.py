#!/usr/bin/env python3

import sys
from simpleLogger import slogger, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
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

    # Note: dsttype isn't actually needed
    sql_query = f"""
    SELECT datasets.filename,files.full_file_path 
    FROM files,datasets 
    WHERE datasets.dsttype in ( '{inbase}' )
      AND datasets.runnumber = {runnumber}
      AND datasets.segment ={segment}
      AND files.lfn=datasets.filename
    ORDER BY datasets.filename
    """
    rows = dbQuery( cnxn_string_map['fcr'], sql_query).fetchall()
    file_list=[]
    full_path_list=[]
    for row in rows:
        filename,full_file_path = row
        file_list.append(filename)
        full_path_list.append(full_file_path)

    if not file_list:
        print("No files found for the given criteria.")

    list_filename = "infile.list"
    full_path_list_filename = "infile_paths.list"
    try:
        with open(list_filename, 'w') as f_out:
            for fname in file_list:
                f_out.write(f"{fname}\n")
    except IOError as e:
            print(f"Error writing to file {list_filename}: {e}")

    try:
        with open(full_path_list_filename, 'w') as f_out:
            for fname in full_path_list:
                f_out.write(f"{fname}\n")
    except IOError as e:
            print(f"Error writing to file {full_path_list_filename}: {e}")
            
if __name__ == "__main__":
    main()
