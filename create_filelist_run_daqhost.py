#!/usr/bin/env python3

import sys
from collections import defaultdict

from sphenixdbutils import cnxn_string_map, dbQuery # type: ignore

def main():
    if len(sys.argv) < 4:
        script_name = sys.argv[0]
        print(f"usage: {script_name} <runnumber> <daqhost> <segswitch>")
        sys.exit(0)

    runnumber_str = sys.argv[1]
    try:
        runnumber = int(runnumber_str)
    except ValueError:
        print(f"Error: runnumber '{runnumber_str}' must be an integer.")
        sys.exit(1)

    daqhost = sys.argv[2]

    # Using a defaultdict to easily append to lists of filenames per host
    file_list_by_host = defaultdict(list)

    ### Important change, 07/15/2025: Usually only care about segment 0!
    segswitch=sys.argv[3]
    sql_query = f"""
    SELECT filename, daqhost
    FROM datasets
    WHERE runnumber = {runnumber}"""
    if segswitch == "seg0fromdb":
        sql_query += "\n\t AND (segment = 0)"
    elif segswitch == "allsegsfromdb":
        pass
    else:
        print("segswitch = {seg0fromdb|allsegsfromdb} must be explicitly provided")
        exit(1)
    sql_query += f"""
    AND (daqhost = '{daqhost}' OR daqhost = 'gl1daq')
    AND status=1
    ORDER BY filename
    """
    print(sql_query)
    rows = dbQuery( cnxn_string_map['rawr'], sql_query).fetchall()
    for row in rows:
        filename, host = row
        file_list_by_host[host].append(filename)

    if not file_list_by_host:
        print("No files found for the given criteria.")

    for host, filenames in file_list_by_host.items():
        list_filename = f"{host}.list"
        try:
            with open(list_filename, 'w') as f_out:
                for fname in filenames:
                    f_out.write(f"{fname}\n")
        except IOError as e:
            print(f"Error writing to file {list_filename}: {e}")

if __name__ == "__main__":
    main()
