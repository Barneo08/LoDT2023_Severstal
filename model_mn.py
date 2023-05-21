import pandas as pd

df = pd.read_csv('E:/Datasets/LoDT2023_Severstal/Y_train_5.csv')

df['DT'] = pd.to_datetime(df['DT'])
df['DT'] = df['DT'].dt.floor('5min')
df_out = df.groupby(['DT']).mean()
df_out.to_csv('E:/Datasets/LoDT2023_Severstal/Y_train_5_to_5minutes.csv', sep='\t')