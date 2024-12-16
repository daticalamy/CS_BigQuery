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
import os

# GitHub API base URL
GITHUB_API_URL = "https://api.github.com"

# Replace with your GitHub repository details
REPO_OWNER = "daticalamy"  # GitHub username or organization
REPO_NAME = "CS_BigQuery"  # Repository name
#BRANCH = "main"  # Branch name, usually 'main'
BRANCH = os.environ.get("CURRENT_BRANCH")
print(f"CURRENT_BRANCH = {BRANCH}")

# GitHub authentication token - Required for private repos
#GITHUB_TOKEN = "github_pat_11....." 
GITHUB_TOKEN = os.environ.get("PYTHON_CHECKS_GITHUB_TOKEN")

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
           
            print(f"TABLE NAME is: {table_name}")
            
            ### For any tables with labels pii, check GitHub repo to ensure views exist for both <table_name>_vw and <table_name>_s.
            
            # These regexes to be used for searching for views with <table_name>_vw and <table_name>_s
            VW_REGEX = f"(?is)create\s*(|or\s*replace\s*)view.*{table_name}_vw"
            S_REGEX = f"(?is)create\s*(|or\s*replace\s*)view.*{table_name}_s"
                        
            VW_VIEW_FOUND = check_regex_in_github_repo(REPO_OWNER, REPO_NAME, BRANCH, VW_REGEX)
            print(f"VW VIEW_FOUND = {VW_VIEW_FOUND}")
            
            S_VIEW_FOUND = check_regex_in_github_repo(REPO_OWNER, REPO_NAME, BRANCH, S_REGEX)
            print(f"S_VIEW_FOUND = {S_VIEW_FOUND}")
            
            if not VW_VIEW_FOUND or not S_VIEW_FOUND:
                liquibase_status.fired = True
                status_message = str(liquibase_utilities.get_script_message()).replace("__TABLE_NAME__", f"\"{table_name}\"")
                liquibase_status.message = status_message
                sys.exit(1)

###
### Default return code
###
False

def check_regex_in_github_repo(owner, repo, branch, regex):
    """
    Checks if a regex pattern is found in any file of a GitHub repository.

    :param owner: The owner of the GitHub repository (e.g., 'octocat')
    :param repo: The name of the repository (e.g., 'Hello-World')
    :param regex: The regex string to search for (e.g., r'\bdef\s+\w+')
    :return: True if the regex is found in any file, False otherwise.
    """

    # GitHub API URL to get the list of files in the repository
    url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    
    response = requests.get(url, headers=headers)
    
    # Check if the request was successful
    if response.status_code != 200:
        print(f"Error retrieving repository: {response.status_code}")
        return False
        
    if response.status_code == 200:
        tree = response.json().get("tree", [])
        files = [file["path"] for file in tree if file["type"] == "blob"]
            
    if files:
        for file_path in files:
            #check_string_in_file(REPO_OWNER, REPO_NAME, file_path)
       
            url = f"{GITHUB_API_URL}/repos/{owner}/{repo}/contents/{file_path}?ref={branch}"
            headers = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}
    
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                content = response.json().get("content")
            if content:
                decoded_content = base64.b64decode(content).decode("utf-8", errors="ignore")

            if re.search(regex, decoded_content):
                print(f"Found '{regex}' in {file_path}")
                return True

    return False