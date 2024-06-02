import ast
import pandas as pd
import pyarrow as pa
import os
import pyarrow.parquet as pq
import glob
'''
extract functions from python files and creating a new parequet file with columns:
    1. function name
    2. function code
    3. code length
    4. file path
    5. repository name
    6. repository url 

    paramters:
        parquet_path: the parquet storing python files content
        threshold: the threshold of code length, if the code length of a function is larger than the threshold, it will be ignored

'''
def extract_functions_from_file(parquet_path, threshold = None):
    df = pd.read_parquet(parquet_path)
    functions = []
    for index, cur_file in df.iterrows():
        file_content = cur_file['content']
        try:
            tree = ast.parse(file_content)
        except SyntaxError as e:
            #print(f"Syntax error in file at index {index}: {e}")
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                length = node.lineno
                if threshold and length > threshold:
                    continue
                func_name = node.name
                func_code = ast.get_source_segment(file_content, node)
                functions.append({
                    'function_name': func_name,
                    'function_code': func_code,
                    'code_length': node.lineno,
                    'file_path': cur_file['file_path'],
                    'repo_name': cur_file['repo_name'],
                    'repo_url': cur_file['repo_url'],
                })
        
    return functions

'''
save the functions table to a parquet file
parameters:
    parent_dir: parent directory to save the parquet file
    functions_table: the table with functions information
    save_path: the detailed path to save the parquet file
'''
def save_parquet_file(parent_dir, functions_table, save_path):
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    df = pd.DataFrame(functions_table)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f'{parent_dir}/{save_path}')

'''
merge parquet chunk files into one file by given path
parameters:
    base_dir: the base directory to find the parquet files
    given_path: the detailed path to search for target parquet files
    output_path: the path to save the combined parquet file
'''
def merge_parquet_files(base_dir, given_path, output_path):
    parquet_files = []
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.parquet') and given_path in os.path.join(root, file):
                parquet_files.append(os.path.join(root, file))
    
    if not parquet_files:
        print(f"No parquet files found with given path: {given_path}")
        return

    combined_df = pd.concat([pd.read_parquet(file) for file in parquet_files])

    combined_df.to_parquet(output_path, index=False)
    print(f"Combined parquet file saved to: {output_path}")

# list of directores to process
query_directories = ['blog', 'data_visualization', 'ecommerce', 'social_media']
for query_dir in query_directories:
    # merge parquet chunk files to one combined file
    base_directory = f'{query_dir}/files_in_chunk'
    search_path_beforegpt = '2021'
    search_path_aftergpt = '2023'
    output_file_path_beforegpt = f'{query_dir}/merged_files/{search_path_beforegpt}.parquet'
    output_file_path_aftergpt = f'{query_dir}/merged_files/{search_path_aftergpt}.parquet'
    if not os.path.exists(f'{query_dir}/merged_files'):
        os.makedirs(f'{query_dir}/merged_files')
    merge_parquet_files(base_directory, search_path_beforegpt, output_file_path_beforegpt)
    merge_parquet_files(base_directory, search_path_aftergpt, output_file_path_aftergpt)

    # get python file parquet paths
    parquet_files = glob.glob(os.path.join(f'{query_dir}/files', '*.parquet'))
    for file_path in parquet_files:
        #threshold = 100
        functions_table = extract_functions_from_file(file_path)
        save_path = os.path.basename(file_path) 
        save_parquet_file(f'{query_dir}/functions', functions_table, save_path)
        print(f'Finished processing {query_dir}/functions/{save_path}')