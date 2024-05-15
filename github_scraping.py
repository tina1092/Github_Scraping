import requests
import json
import git

'''
    Scraping Github repositories using Github API
        token: Github personal access token
        language: Programming language to search for
        time_year: Year to search for
        time_month: Month to search for
        parent_dir: Parent directory to save the repositories
        download_limit: Number of repositories to download
        additional_query: Additional query parameters to search for
'''
def github_scraping(token, language, time_year, time_month, parent_dir, download_limit = 1, additional_query = ''):
    query = f'language:{language} {additional_query} created:{time_year}-{time_month}-01..{time_year}-{time_month}-30'
    url = f'https://api.github.com/search/repositories?q={query}&per_page=100'
    headers = {'Authorization': f'token {token}'}

    '''
        Collecting information from Github API using the given url and token headers
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
            
            if 'next' in response.links:
                url = response.links['next']['url']
            else:
                url = None
            if len(repos) >= download_limit:
                break
        return repos


    repositories = fetch_repositories(url, headers)
    

    #Saving the repositories to a json file
    with open(f'{parent_dir}.json', 'w') as f:
        json.dump(repositories, f, indent=4)

    print(f'Total repositories fetched: {len(repositories)}')

    clone_url_list = []
    for repo in repositories:
        clone_url = repo.get('clone_url')
        clone_url_list.append(clone_url)


    for index, repo_url in enumerate(clone_url_list):
        try:
            if index >= download_limit:
                break
            repository_name = repo_url.split('/')[-1].replace('.git', '')
            clone_dir = f'{parent_dir}/{repository_name}'
            print(f'Cloning {repo_url} to {repository_name}')
            git.Repo.clone_from(repo_url, clone_dir)
        except:
            print(f'Error cloning {clone_url}')

# Example
token = 'YOUR_GITHUB_TOKEN'
time_year = '2023'
time_month = '11'
language = 'Python'
parent_dir = 'web_implementation'
additional_query = 'web OR flask OR django'
github_scraping(token, language, time_year, time_month, parent_dir)