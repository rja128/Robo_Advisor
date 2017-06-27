
import pandas as pd
import pandas_datareader.data as web
import os
import pickle
import requests
import csv

def get_AMERICAN_Stock_List():
        if not os.path.exists('Stock_Data'):
                os.makedirs('Stock_Data')

def list_AMER():

	dfNYSE = pd.read_csv('Stock_Data/Lists/NYSE_stocks.csv')
	dfNASDAQ = pd.read_csv('Stock_Data/Lists/NASDAQ_stocks.csv')
	dfAMEX = pd.read_csv('Stock_Data/Lists/AMEX_stocks.csv')

	holder = [dfNYSE, dfNASDAQ, dfAMEX]

	dfAMERICA = pd.concat(holder)

	dfAMERICA = dfAMERICA.reset_index()

	del dfAMERICA['index']

	return dfAMERICA

df1 = list_AMER()

for x in df1.itertuples():
	##print(x[7])
	if x[7]== 'Technology':
		print(x[0], " ", x[1], " ", x[7])

df2 = df1.drop(0)
df2 = df2.drop(3)
df2 = df2.drop(5)


print(df2)
