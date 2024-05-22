import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import seaborn as sns
import glob
'''
plot histogram by given python file

paramters:
        df: pandas dataframe, contains the code length of each function
        parent_dir: the parent directory to save histogram
'''
def plot_histogram(df, parent_dir):
        grouped = df.groupby('file_path')
        bins = [0,11, 21, 31, 41, 51, 61, 71, 81, 91, 101, 201, 301, 401]
        xticks = bins
        xtick_labels = ['1-10', '11-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80', '81-90', '91-100', '101-200', '201-300', '300+']
        save_parent_path = f'{parent_dir}/histogram'
        if not os.path.exists(save_parent_path):
                os.makedirs(save_parent_path)
        for file_path, group in grouped:
                code_lengths = group['code_length'].apply(lambda x: min(x, 301)).tolist()
                counts, bin_edges = np.histogram(code_lengths, bins=bins)
                df_hist = pd.DataFrame({
                'bin': pd.cut(code_lengths, bins=bins, labels=xtick_labels, right=False),
                'count': code_lengths
                }) 
                df_hist = df_hist.groupby('bin').size().reset_index(name='count') 
                plt.figure(figsize=(12, 6))
                sns.barplot(x='bin', y='count', data=df_hist, color="skyblue")
                plt.xlabel('Code Length')
                plt.ylabel('Count')
                max_count = df_hist['count'].max()
                plt.yticks(np.arange(0, max_count + 1, step=1))
                plt.title(file_path)
                save_path = file_path.replace('/', '_')
                save_path = save_path.rsplit('.', 1)[0]
                plt.savefig(f'{save_parent_path}/{save_path}_histogram.png')
                plt.close()

# list of directores to process
query_directories = ['blog', 'data_visualization', 'ecommerce', 'social_media']

# loop through directories and plot histograms
for query_dir in query_directories:
        parquet_files = glob.glob(os.path.join(query_dir, 'functions/*.parquet'))
        df = pd.read_parquet(parquet_files)
        plot_histogram(df, query_dir)
        print(f'Finished plotting histograms for {query_dir}')