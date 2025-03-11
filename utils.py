import re
def extract_numbers_to_commastring(filepath):
    """
    Extracts all numbers from a file, combines them into a comma-separated string,
    and returns the string. Numbers can be separated by whitespace including newlines.

    Args:
        filepath: The path to the file.

    Returns:
        A string containing a comma-separated list of numbers, or None if the file
        does not exist or no numbers are found.
    """
    try:
        with open(filepath, 'r') as file:
            content = file.read()
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None

    # Find all integer numbers. Could catch mistakes better
    numbers = re.findall(r"[-+]?\d+", content)
    return ','.join(numbers) if numbers else None
        
# ============================================================================================
def list_to_condition(lst, name) :
    """
    Generates a SQL-like condition string from a list of values.

    This function takes a list (`lst`) and a field name (`name`) and constructs a 
    string that can be used as a `WHERE` clause condition in a SQL query. It 
    handles different list lengths to create appropriate conditions.
    No effort is made to ensure that inputs are numbers and properly ordered.

    Args:
        lst: A list of values (e.g., run numbers, segment numbers).
        name: The name of the field/column in the database (e.g., "runnumber", "segment").

    Returns:
        A string representing a SQL-like condition, or None if the list is empty.

    Examples:
        - list_to_condition([123], "runnumber") returns "and runnumber=123"
        - list_to_condition([100, 200], "runnumber") returns "and runnumber>=100 and runnumber<=200"
        - list_to_condition([1, 2, 3], "runnumber") returns "and runnumber in ( 1,2,3 )"
        - list_to_condition([], "runnumber") returns None
    """
    condition = ""
    if   len( lst )==1:
        condition = f"and {name}={lst[0]}"
    elif len( lst )==2:
        condition = f"and {name}>={lst[0]} and {name}<={lst[1]}"
    elif len( lst )>=3 :
        condition = f"and {name} in ( %s )" % ','.join( lst )
    else: 
        return None

    return condition


        
