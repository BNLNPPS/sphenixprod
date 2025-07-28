#!/usr/bin/env python3

"""
Generate a list of files (and one with full paths) with given specifications.
Note: It should only find one file every time.
"""

import sys
from simpleLogger import slogger, CHATTY, DEBUG, INFO, WARN, ERROR, CRITICAL  # noqa: F401
from sphenixdbutils import cnxn_string_map, dbQuery # type: ignore

def main():
    slogger.setLevel("DEBUG")
    script_name = sys.argv[0]
    if len(sys.argv) == 4 : 
        WARN(f"Deprecated usage of {script_name}. Please use this signature in the future:")
        WARN(f"usage: <dataset> <tag> <dsttype> <runnumber> <segment> ")
        dsttype = sys.argv[1]
        runnumber_str = sys.argv[2]
        segment_str = sys.argv[3]
    elif len(sys.argv) == 6 :
        dataset = sys.argv[1]
        tag     = sys.argv[2]
        dsttype = sys.argv[3]
        runnumber_str = sys.argv[4]
        segment_str = sys.argv[5]
    else:
        ERROR(f"usage: [dataset] [tag] [dsttype] <runnumber> <segment> ")                
        sys.exit(1)

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
    WHERE datasets.dsttype in ( '{dsttype}' )"""
    #### This should _always_ be provided. Only leaving it optional for backward  compatibility
    if len(sys.argv) == 6 :
        sql_query += f"""
      AND datasets.tag='{tag}'
      AND datasets.dataset = '{dataset}' """
    ### Rest of query
    sql_query += f"""
      AND datasets.runnumber = {runnumber}
      AND datasets.segment ={segment}
      AND files.lfn=datasets.filename
    ORDER BY datasets.filename
    ;"""
    DEBUG (f"datasets query is {sql_query}")
    rows = dbQuery( cnxn_string_map['fcr'], sql_query).fetchall()
    file_list=[]
    full_path_list=[]
    for row in rows:
        file_list.append(row.filename)
        full_path_list.append(row.full_file_path)

    if not file_list:
        print("No files found for the given criteria.")
        exit(0)
        
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
                DEBUG(f"Adding {fname}")
                f_out.write(f"{fname}\n")
    except IOError as e:
            print(f"Error writing to file {full_path_list_filename}: {e}")
            
if __name__ == "__main__":
    main()
