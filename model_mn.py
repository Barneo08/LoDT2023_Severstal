import pandas as pd

df = pd.read_csv('E:/Datasets/LoDT2023_Severstal/y_train_4.csv')

df_out = df[df.DT == '2019-05-18 08:26:50']

print(df_out.head())