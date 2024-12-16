###
### This script checks for numeric characters in VARCHAR columns
###
### Notes:
### 1. Only basic INSERT or UPDATE statements are supported
### 2. Inserting multiple rows within same INSERT is not supported
### 3. LoadData change types are not supported by checks
###

###
### Helpers come from Liquibase
###
import script_helper
import shlex
import sys
import re

###
### Functions
###
def check_data(string_data):
    """Returns True if data is valid."""
    return not any(char.isdigit() for char in string_data)

def find_snapshot_object(object_list, type, key, value):
    """Returns a snapshot object given a key (e.g., name) and attribute."""
    for object in object_list:
        if object[type][key].lower() == value.lower():
            return object
    return None

def parse_parameters(string_data, whitespace=","):
    """Returns a list containing the string separated by whitespace characters."""
    lex = shlex.shlex(string_data, posix=True)
    lex.whitespace += whitespace
    return [data for data in list(lex)]

###
### main
###

###
### Retrieve log handler
### Ex. liquibase_logger.info(message)
###
liquibase_logger = script_helper.get_logger()

###
### Retrieve status handler
###
liquibase_status = script_helper.get_status()

###
### Retrieve JSON snapshot
###
liquibase_snapshot = script_helper.get_snapshot()

###
### Exit if column or table data is missing
###
if not all(key in liquibase_snapshot["snapshot"]["objects"] for key in ("liquibase.structure.core.Column", "liquibase.structure.core.Table")):
    liquibase_status.fired = False
    liquibase_logger.warning("Column or Table data missing from snapshot. Check skipped.")
    sys.exit(1)

###
### Retrieve columns and tables from snapshot
###
all_columns = liquibase_snapshot["snapshot"]["objects"]["liquibase.structure.core.Column"]
all_tables = liquibase_snapshot["snapshot"]["objects"]["liquibase.structure.core.Table"]

###
### Retrieve all changes in changeset
###
changes = script_helper.get_changeset().getChanges()

###
### Loop through all changes
###
for change in changes:
    ###
    ### LoadData change types are not currently supported
    ###
    if "loaddatachange" in change.getClass().getSimpleName().lower():
        liquibase_logger.info("LoadData change type not supported. Statement skipped.")
        continue
    ###
    ### Retrieve sql as string, remove extra whitespace
    ###
    raw_sql = script_helper.strip_comments(script_helper.generate_sql(change)).casefold()
    raw_sql = " ".join(raw_sql.split())
    ###
    ### Split sql into statements
    ###
    raw_statements = script_helper.split_statements(raw_sql)
    for raw_statement in raw_statements:
        column_dict = {}
        data_list = []
        ###
        ### Split raw_statement into list
        ###
        sql_list = raw_statement.split()
        try:
            command_name = sql_list[0]
            if command_name == "insert":
                table_name = sql_list[2]
            elif command_name == "update":
                table_name = sql_list[1]
            else:
                raise UserWarning
        except UserWarning:
            liquibase_logger.info(f"Non Insert/Update statement skipped: {raw_statement}")
            continue
        ###
        ### Remove schema if provided, locate table
        ### Add code to support "(" character immediately following tablename, eg. INSERT INTO TABLE(field1, ...
        ###
        table_name = table_name.split(".")[-1].split("(", 1)[0]
        ### liquibase_logger.info(f"Table being evaluated: {table_name}")
        table_object = find_snapshot_object(all_tables, "table", "name", table_name)
        if table_object is None:
            liquibase_logger.warning(f"Table \"{table_name}\" not found in snapshot. Statement skipped.")
            continue
        ###
        ### INSERT
        ###
        if command_name == "insert":
            search_string = fr"{table_name}\s*\("
            matches = re.finditer(search_string, raw_statement, re.MULTILINE | re.IGNORECASE)
            
            ###
            ### INSERT INTO TABLE (column1, column2, ...) VALUES (value1, value2, ...)
            ###
            for matchNum, match in enumerate(matches, start=1):
                ### print ("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum = matchNum, start = match.start(), end = match.end(), match = match.group()))
                end = raw_statement.find(")", match.end())
                if end != -1:
                    column_list_names = parse_parameters(raw_statement[match.end():end])
                    for column_name in column_list_names:
                        column_object = find_snapshot_object(all_columns, "column", "name", column_name)
                        if column_object is not None:
                            column_dict[column_object["column"]["name"]] = column_object["column"]["type"]["typeName"].lower()
                break
            ###
            ### INSERT INTO TABLE VALUES (value1, value2, ...)
            ###
            else:
                column_list_ids = [column_id.replace("liquibase.structure.core.Column#", "") for column_id in table_object["table"]["columns"]]
                for column_id in column_list_ids:
                    column_object = find_snapshot_object(all_columns, "column", "snapshotId", column_id)
                    if column_object is not None:
                        column_dict[column_object["column"]["name"]] = column_object["column"]["type"]["typeName"].lower()
            
            ###
            ### Process data
            ###
            search_string = r"values\s*\("
            matches = re.finditer(search_string, raw_statement, re.MULTILINE | re.IGNORECASE)
            
            for matchNum, match in enumerate(matches, start=1):
                ### print ("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum = matchNum, start = match.start(), end = match.end(), match = match.group()))
                end = raw_statement.rfind(")")
                if end != -1:
                    data_list = parse_parameters(raw_statement[match.end():end])

        ###
        ### UPDATE
        ###
        else:
            search_string = "set "
            start = raw_statement.find(search_string)
            ###
            ### UPDATE TABLE SET column1 = value1, column2 = value2, ...
            ###
            if start != -1:
                start += len(search_string)
                end = raw_statement.rfind("where")
                if end == -1:
                    end = None
                combined_data = parse_parameters(raw_statement[start:end], ",=")
                for index in range(len(combined_data)):
                    if index % 2 == 0:
                        column_object = find_snapshot_object(all_columns, "column", "name", combined_data[index])
                        if column_object is not None:
                            column_dict[column_object["column"]["name"]] = column_object["column"]["type"]["typeName"].lower()
                    else:
                        data_list.append(combined_data[index])
        ###
        ### Continue to next statement if columns are empty or column/data counts don't match
        ###
        if len(column_dict) == 0 or len(column_dict) != len(data_list):
            liquibase_logger.warning("Column/data count mismatch. Statement skipped.")
            continue
        ###
        ### Merge columns and data
        ###
        merged_data = {}
        for (key, value), data in zip(column_dict.items(), data_list):
            merged_data[key] = {"data":data, "type":value}
        ###
        ### Check for numeric characters in varchar columns
        ###
        for key in merged_data:
            if "varchar" in merged_data[key]["type"]:
                if not check_data(merged_data[key]["data"]):
                    liquibase_status.fired = True
                    status_message = str(script_helper.get_script_message()).replace("__COLUMN_NAME__", f"\"{key}\"")
                    liquibase_status.message = status_message
                    sys.exit(1)

###
### Default return code
###
False