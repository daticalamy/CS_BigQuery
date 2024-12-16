###
### This script checks that any CREATE TABLE with a PII label has a corresponding PII and non-PII view.
###

###
### Helpers come from Liquibase
###
import sys
import liquibase_utilities
import re
import requests
import base64

# GitHub API base URL
GITHUB_API_URL = "https://api.github.com"

# Replace with your GitHub repository details
REPO_OWNER = "daticalamy"  # GitHub username or organization
REPO_NAME = "CS_BigQuery"  # Repository name
BRANCH = "main"  # Branch name, usually 'main' or 'master'

# GitHub authentication token (optional but recommended for higher rate limits)
GITHUB_TOKEN = "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"  # Optional, if you have a GitHub token

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
            
            # The string to search for
            SEARCH_STRING = f"CREATE or replace VIEW TEST_SET_01.{table_name}_vw"
            print(f"SEARCH_STRING is: {SEARCH_STRING}")
            
            search_string_in_repo()
            
            if not table_name.isupper():
                liquibase_status.fired = True
                status_message = str(liquibase_utilities.get_script_message()).replace("__TABLE_NAME__", f"\"{table_name}\"")
                liquibase_status.message = status_message
                sys.exit(1)

###
### Default return code
###
False


# Function to get files in the repository
def get_repo_files(owner, repo, branch):
    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        tree = response.json().get("tree", [])
        files = [file["path"] for file in tree if file["type"] == "blob"]
        return files
    else:
        print(f"Error fetching repository files: {response.status_code}")
        return []

# Function to check if the string is in a file
def check_string_in_file(owner, repo, file_path):
    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/contents/{file_path}?ref={BRANCH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        content = response.json().get("content")
        if content:
            decoded_content = base64.b64decode(content).decode("utf-8", errors="ignore")
            if SEARCH_STRING in decoded_content:
                print(f"Found '{SEARCH_STRING}' in {file_path}")
#            else:
#                print(f"'{SEARCH_STRING}' not found in {file_path}")
        else:
            print(f"Error decoding content for file: {file_path}")
    else:
        print(f"Error fetching file content for {file_path}: {response.status_code}")

# Main function to query the repo and search for the string
def search_string_in_repo():
    files = get_repo_files(REPO_OWNER, REPO_NAME, BRANCH)
    
    if files:
        for file_path in files:
            check_string_in_file(REPO_OWNER, REPO_NAME, file_path)
