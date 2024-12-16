###
### This script checks that any CREATE TABLE with a PII label has a corresponding PII and non-PII view.
###

###
### Helpers come from Liquibase
###
import sys
import liquibase_utilities
import re

###
### Retrieve log handler
### Ex. liquibase_logger.info(message)
###
liquibase_logger = liquibase_utilities.get_logger()

###
### Retrieve status handler
###
liquibase_status = liquibase_utilities.get_status()

###
### Retrieve all changes in changeset
###
changes = liquibase_utilities.get_changeset().getChanges()

###
### Loop through all changes
###
for change in changes:
    ###
    ### LoadData change types are not currently supported
    ###
    if "loaddatachange" in change.getClass().getSimpleName().lower():
        continue
    ###
    ### Split sql into a list of strings to remove whitespace
    ###
    sql_list = liquibase_utilities.generate_sql(change).split()
    ### DEBUG print(f"SQL_LIST is: {sql_list}")
    ###
    ### Locate create (or replace) table in list that additionally contains a labels value of pii.
    ###
    if "create" in map(str.casefold, sql_list) and "table" in map(str.casefold, sql_list) and any(re.search(r"labels.*pii", item) for item in map(str.casefold, sql_list)):    
        index_table = [token.lower() for token in sql_list].index("table")
        if index_table + 1 < len(sql_list):
            table_name = sql_list[index_table + 1]
            
            ### Format table name to remove dataset or any ` characters.
            if '.' in table_name:
              table_name = table_name.split('.', 1)[1]
            table_name = table_name.replace("`", "")
           
            ### DEBUG print(f"TABLE NAME is: {table_name}")
            
            ### For any tables with labels pii, check GitHub repo to ensure views exist for both <table_name>_vw and <table_name>_s.
            
            
            
            
            
            
            
            
            
            
            
            
            
            if not table_name.isupper():
                liquibase_status.fired = True
                status_message = str(liquibase_utilities.get_script_message()).replace("__TABLE_NAME__", f"\"{table_name}\"")
                liquibase_status.message = status_message
                sys.exit(1)

###
### Default return code
###
False
