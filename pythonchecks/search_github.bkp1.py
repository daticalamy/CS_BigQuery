import requests
import base64
import re

# GitHub API base URL
GITHUB_API_URL = "https://api.github.com"

# Replace with your GitHub repository details
REPO_OWNER = "daticalamy"  # GitHub username or organization
REPO_NAME = "CS_BigQuery"  # Repository name
BRANCH = "main"  # Branch name, usually 'main' or 'master'

# The string to search for
SEARCH_STRING = "CREATE or replace TABLE"

# GitHub authentication token (optional but recommended for higher rate limits)
GITHUB_TOKEN = "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"  # Optional, if you have a GitHub token

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

if __name__ == "__main__":
    search_string_in_repo()