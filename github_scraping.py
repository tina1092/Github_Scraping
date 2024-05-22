import requests
import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path

'''
    Scraping Github repositories and creating a parquet file with columns:
    1. code content
    2. file timestamp
    3. file path name
    4. file type
    5. repository name
    6. repository url  

    paramters:
        token: Github personal access token
        language: Programming language to search for
        file_extension: File extension to search for, usually same as language
        time_year: Year to search for
        time_month: Month to search for
        parent_dir: Parent directory to save the repositories
        file_name: Name of the parquet file
        afterGPT: True if we want to scrape repositories after ChatGPT was released
        download_limit: Number of repositories to download
        additional_query: Additional query parameters to search for
        
'''
def github_scraping(token, language, file_extension, time_year, time_month, 
                    parent_dir, file_name, afterGPT, download_limit = 1, additional_query = '', detailed_check_commit = False):
    # format variables
    time_month = str(time_month).zfill(2)
    if not isinstance(file_extension, list):
        file_extension = [file_extension]
    query = f'language:{language} {additional_query} created:{time_year}-{time_month}-01..{time_year}-{time_month}-30'
    url = f'https://api.github.com/search/repositories?q={query}&per_page=100'
    headers = {'Authorization': f'token {token}'}

    '''
        Collecting repository information
    '''
    def fetch_repositories(url, headers):
        repos = []
        while url:
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f'Error: {response.status_code}')
                break
            result = response.json()
            repos.extend(result.get('items', []))
            url = response.links.get('next', {}).get('url')
            if len(repos) >= download_limit:
                break
        return repos[:download_limit]
    repositories = fetch_repositories(url, headers)

    '''
        Collecting files information in given repository
    '''
    def fetch_files(repo, headers):
        py_files = []
        repo_name = repo['full_name']
        repo_url = repo['html_url']
        contents_url = f'https://api.github.com/repos/{repo_name}/contents'

        def parse_contents(url):
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f'Error fetching contents for {repo_name}: {response.status_code}')
                return
            items = response.json()
            for item in items:
                if item['type'] == 'file' and any(item['name'].endswith(ext) for ext in file_extension):
                    file_response = requests.get(item['download_url'], headers=headers)
                    commit_url =  f'https://api.github.com/repos/{repo_name}/commits?path={item["path"]}'
                    # if detailed_check_commit is True, we need to loop through all commits
                    if detailed_check_commit:
                        all_commits = []
                        while commits_url:
                            commit_response = requests.get(commits_url, headers=headers)
                            if commit_response.status_code != 200:
                                break
                            commit_info = commit_response.json()
                            all_commits.extend(commit_info)
                            commits_url = commit_response.links.get('next', {}).get('url')
                        
                        if file_response.status_code == 200:
                            for cur_commit in all_commits:
                                timestamp = cur_commit['commit']['committer']['date']
                                time = datetime.fromisoformat(timestamp[:-1])
                                if afterGPT or time < datetime(2022, 11, 1):
                                    py_files.append({
                                        'content': file_response.text,
                                        'timestamp': timestamp,
                                        'file_path': item['path'],
                                        'repo_name': repo_name,
                                        'repo_url': repo_url,
                                    })
                                    break
                    else:  
                        commit_response = requests.get(commit_url, headers=headers)
                        if file_response.status_code == 200 and commit_response.status_code == 200:
                            commit_info = commit_response.json()
                            for cur_commit in commit_info:
                                timestamp = cur_commit['commit']['committer']['date']
                                time = datetime.fromisoformat(timestamp[:-1])
                                '''
                                If we want to find the file before ChatGPT was released,
                                we need to loop the commit logs until find the commit date 
                                before the ChatGPT release (setting the date to 2022-11-01)
                                '''
                                if afterGPT or time < datetime(2022, 11, 1):
                                    py_files.append({
                                        'content': file_response.text,
                                        'timestamp': timestamp,
                                        'file_path': item['path'],
                                        'file_type': Path(item['name']).suffix.lstrip('.'),
                                        'repo_name': repo_name,
                                        'repo_url': repo_url,
                                    })
                                    break
                        
                elif item['type'] == 'dir':
                    parse_contents(item['url'])

        parse_contents(contents_url)
        return py_files


    all_py_files = []
    # loop through repositories and fetch files
    for repo in repositories:
        py_files = fetch_files(repo, headers)
        all_py_files.extend(py_files)
        
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    if not os.path.exists(f"{parent_dir}/files"):
        os.makedirs(f"{parent_dir}/files")
    # save to parquet file
    df = pd.DataFrame(all_py_files)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f'{parent_dir}/files/{file_name}.parquet')

    print(f'Total files fetched: {len(all_py_files)}')
    return all_py_files

# Example
token = 'YOUR_GITHUB_TOKEN'
time_year = '2023'
time_month = '9'
language = 'Python'
parent_dir = 'blog'
file_name = 'python_files'
additional_query = 'blog'
afterGPT = True
file_extension = ['.py', '.ipynb']
download_limit = 10
detailed_check_commit = False
table = github_scraping(token, language, file_extension, time_year, time_month, 
                        parent_dir, file_name, afterGPT, download_limit, additional_query, detailed_check_commit)
