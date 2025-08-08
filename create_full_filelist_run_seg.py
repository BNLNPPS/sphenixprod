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
        WARN( "usage: <dataset> <intriplet> <dsttype> <runnumber> <segment> ")
        dsttype = sys.argv[1]
        runnumber_str = sys.argv[2]
        segment_str = sys.argv[3]
    elif len(sys.argv) == 6 :
        dataset = sys.argv[1]
        intriplet = sys.argv[2]
        dsttype = sys.argv[3]
        runnumber_str = sys.argv[4]
        segment_str = sys.argv[5]
    else:
        ERROR( "usage: [dataset] [intriplet] [dsttype] <runnumber> <segment> ")                
        sys.exit(1)

    try:
        runnumber = int(runnumber_str)
        segment = int(segment_str)
    except ValueError:
        print(f"Error: runnumber '{runnumber_str}' must be an integer.")
        print(f"     : segment '{segment_str}' must be an integer.")
        sys.exit(1)

    # dsttype comes as a a comma-separated list. Add ticks and parens
    dsttype4sql=dsttype.replace(",","','")    
    
    # Why not the following, you ask?
    # SELECT datasets.filename,files.full_file_path 
    # FROM files,datasets 
    # WHERE files.lfn=datasets.filename
    # Because it's very slow. So split it into separate queries.
    datasets_query = f"""
    SELECT filename
    FROM datasets 
    WHERE datasets.dsttype in ( '{dsttype4sql}' )
    AND datasets.runnumber = {runnumber}
    AND datasets.segment = {segment} """
    #### This should _always_ be provided. Only leaving it optional for backward  compatibility
    if len(sys.argv) == 6 :
        datasets_query += f"""
      AND tag='{intriplet}'
      AND dataset = '{dataset}'"""
    datasets_query += ";"
    
    print (f"datasets query is {datasets_query}")
    rows = dbQuery( cnxn_string_map['fcr'], datasets_query).fetchall()
    file_list=[]
    for row in rows:
        file_list.append(row.filename)

    if not file_list:
        print("No files found for the given criteria.")
        exit(1)
    filelist=sorted(file_list)
    
    ### Collect full paths. Note, we can make this optional for combiner jobs.
    filelist_str="','".join(filelist)
    files_query = f"""
    SELECT full_file_path
    FROM files
    WHERE lfn in ( '{filelist_str}' )
    ;"""
    print (f"files query is {files_query}")
    rows = dbQuery( cnxn_string_map['fcr'], files_query).fetchall()
    full_path_list=[]
    for row in rows:
        full_path_list.append(row.full_file_path)

    if not full_path_list:
        print("No files found for the given criteria.")
        exit(1)
        
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
                # print(f"Adding {fname}")
                f_out.write(f"{fname}\n")
    except IOError as e:
            print(f"Error writing to file {full_path_list_filename}: {e}")
            
if __name__ == "__main__":
    main()
