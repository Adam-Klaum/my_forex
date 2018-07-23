import pandas as pd

#df1 = pd.read_csv('EUR_USD_2013.csv')
df2 = pd.read_csv('EUR_USD_2014.csv')
df3 = pd.read_csv('EUR_USD_2015.csv')
df4 = pd.read_csv('EUR_USD_2016.csv')
df5 = pd.read_csv('EUR_USD_2017.csv')
df6 = pd.read_csv('EUR_USD_2018.csv')


df7 = pd.concat([df2, df3, df4, df5, df6], ignore_index=True)

df7.to_csv('EUR_USD_FULL.csv')

