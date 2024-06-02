import requests
import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
import re
import base64
import time
from collections import deque

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
def github_scraping(token, language, file_extension, time_year, time_start_month, time_end_month,
                    parent_dir, file_name, afterGPT, ignore_filedir, download_limit = 1, additional_query = ''):
    # format variables
    time_start_month = str(time_start_month).zfill(2)
    time_end_month = str(time_end_month).zfill(2)
    if not isinstance(file_extension, list):
        file_extension = [file_extension]

    if not isinstance(token, list):
        token = [token]
    query = f'language:{language} {additional_query} created:{time_year}-{time_start_month}-01..{time_year}-{time_end_month}-30'
    url = f'https://api.github.com/search/repositories?q={query}&per_page=100'
    header_queue = deque()
    for t in token:
        header = {'Authorization': f'token {t}'}
        header_queue.append(header)
    headers = {'Authorization': f'token {token[0]}'}
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

    def show_request_status(token):
        def check_rate_limit(token):
            url = 'https://api.github.com/rate_limit'
            headers = {'Authorization': f'token {token}'}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                rate_limit_info = response.json()
                return rate_limit_info
            else:
                print(f'Error: {response.status_code}')
                return None


        rate_limit_info = check_rate_limit(token)

        if rate_limit_info:
            core_limit = rate_limit_info['resources']['core']
            search_limit = rate_limit_info['resources']['search']
            
            print(f"Core Limit: {core_limit['remaining']} remaining out of {core_limit['limit']}")
            print(f"Search Limit: {search_limit['remaining']} remaining out of {search_limit['limit']}")
            print(f"Core Limit resets at: {datetime.fromtimestamp(core_limit['reset'])}")
            print(f"Search Limit resets at: {datetime.fromtimestamp(search_limit['reset'])}")


    '''
        Collecting files information in given repository
    '''
    def fetch_files(repo, header_queue):
        py_files = []
        repo_name = repo['full_name']
        repo_url = repo['html_url']
        contents_url = f'https://api.github.com/repos/{repo_name}/contents'
        
        def get_next_header():
            header = header_queue.popleft()
            header_queue.append(header)
            return header
        
        headers = header_queue[-1]

        def ignore_dir(curpath):
            lower_path = curpath.rsplit('/', 1)[-1].lower()
            if lower_path in ignore_filedir:
                return True
            if re.match(r'^venv\d+$', lower_path):
                return True
            if re.match(r'^env\d+$', lower_path):
                return True
            return False
        
        '''
        This error happen when the token exceed the search limit
        '''
        def error403(response, headers, url):
            reset_time = response.headers.get('X-RateLimit-Reset')
            if reset_time:
                wait_time = int(reset_time)-int(time.time()) + 1
            else:
                wait_time = 60  # Default wait time if header is missing
            print(f"Rate limit exceeded for {headers}. need to wait for {wait_time} seconds. will wait 60 seconds and try next header")
            time.sleep(3)
            next_headers = get_next_header()
            parse_contents(url, next_headers)  # Retry after waiting
            return
        
        def parse_contents(url, headers):
            response = requests.get(url, headers=headers)
            if response.status_code == 403:
                error403(response, headers, url)
                return
            if response.status_code != 200:
                print(f'Error fetching contents for {repo_name}: {response.status_code}')
                return
            items = response.json()
            for item in items:
                curpath = item['path']
                cur_download_url = item['download_url']
                if ignore_dir(curpath):
                    continue
                if item['type'] == 'file' and any(item['name'].endswith(ext) for ext in file_extension):
                    file_response = requests.get(cur_download_url, headers=headers)
                    commits_url = f'https://api.github.com/repos/{repo_name}/commits?path={item["path"]}&per_page=100'
                    commit_response = requests.get(commits_url, headers=headers)
                    if file_response.status_code == 403:
                        error403(file_response, headers, url)
                        return
                    if commit_response.status_code == 403:
                        error403(commit_response, headers, url)
                        return
                    if file_response.status_code != 200:
                        print(f'Error fetching file for {cur_download_url}: {file_response.status_code}')
                        return
                    if commit_response.status_code != 200:
                        print(f'Error fetching commit for {cur_download_url}: {commit_response.status_code}')
                        return
                    
                    commit_info = commit_response.json()
                    find = False
                    for cur_commit in commit_info:
                        timestamp = cur_commit['commit']['committer']['date']
                        cur_commit_time = datetime.fromisoformat(timestamp[:-1])
                        '''
                        If we want to find the file before ChatGPT was released,
                        we need to loop the commit logs until find the commit date 
                        before the ChatGPT release (setting the date to 2022-11-01)
                        '''
                        if afterGPT:
                            py_files.append({
                                'content': file_response.text,
                                'timestamp': timestamp,
                                'file_path': item['path'],
                                'repo_name': repo_name,
                                'repo_url': repo_url,
                            })
                            print('|', end='')
                            break
                        elif cur_commit_time < datetime(2022, 11, 1):
                            find = True
                            file_url = f'https://api.github.com/repos/{repo_name}/contents/{item["path"]}?ref={cur_commit["sha"]}'
                            cur_file_response = requests.get(file_url, headers=headers)
                            if cur_file_response.status_code == 403:
                                error403(cur_file_response, headers, url)
                                return
                            if cur_file_response.status_code != 200:
                                print(f'Error fetching current commit file for {file_url}: {cur_file_response.status_code}')
                                return
                            
                            cur_file_content = cur_file_response.json()
                            try:
                                file_data = base64.b64decode(cur_file_content['content']).decode('utf-8')
                            except UnicodeDecodeError:
                                print(f'Error decoding file for {file_url}')
                                continue
                            py_files.append({
                                'content': file_data,
                                'timestamp': timestamp,
                                'file_path': item['path'],
                                'repo_name': repo_name,
                                'repo_url': repo_url,
                            })
                            print('|', end='')
                            break
                    if not afterGPT and not find:
                        print("TOO MANY COMMITS")
                elif item['type'] == 'dir':
                    parse_contents(item['url'],headers)


        parse_contents(contents_url, headers)
        return py_files

    chunk_index_size = 4
    chunk_size = int(len(repositories)/chunk_index_size)
    chunks = [repositories[i:i + chunk_size] for i in range(0, len(repositories), chunk_size)]
    total_py_files = []
    
    
    for chunk_index in range(chunk_index_size):
        all_py_files = []
        progress_index = 0
        chunk_size = len(chunks[chunk_index])
        # loop through repositories and fetch files
        for repo in chunks[chunk_index]:
            py_files = fetch_files(repo, header_queue)
            print()
            for t in token:
                show_request_status(t)
                print()
            all_py_files.extend(py_files)
            progress_index += 1
            print(f'Progress: {progress_index}/{chunk_size}')
            print("++++++++++++++++++++++++++++")

        if not os.path.exists(f"{parent_dir}/files_in_chunk"):
            os.makedirs(f"{parent_dir}/files_in_chunk")
        # save to parquet file
        df = pd.DataFrame(all_py_files)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f'{parent_dir}/files_in_chunk/{file_name}_chunk{chunk_index}.parquet')

        print(f'Total files fetched: {len(all_py_files)}')
        total_py_files.extend(all_py_files)
    return total_py_files

# Example
token = ['YOUR_GITHUB_TOKEN']
time_year = '2023'
time_start_month = '8'
time_end_month = '12'
language = 'Python'
parent_dir = 'math'
file_name = '2023_aug_dec_python'
additional_query = 'math'
afterGPT = True
file_extension = ['.py', '.ipynb']
download_limit = 4

ignore_filedir = ['venv', '.git', '.idea', '__pycache__', 'image', 'images','lib','libs', 'env', 'img']

table = github_scraping(token, language, file_extension, time_year, time_start_month, time_end_month,
                        parent_dir, file_name, afterGPT, ignore_filedir, download_limit, additional_query)
