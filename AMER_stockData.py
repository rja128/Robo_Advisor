import datetime as dt
import pandas as pd
import pandas_datareader.data as web
import os
import pickle
import requests
import csv

def get_AMERICAN_Stock_List():
	if not os.path.exists('Stock_Data'): 
		os.makedirs('Stock_Data')

def create_Full_List():
	
	dfNYSE = pd.read_csv('Stock_Data/Lists/NYSE_stocks.csv')
	dfNASDAQ = pd.read_csv('Stock_Data/Lists/NASDAQ_stocks.csv')
	dfAMEX = pd.read_csv('Stock_Data/Lists/AMEX_stocks.csv')

	holder = [dfNYSE, dfNASDAQ, dfAMEX]

	dfAMERICA = pd.concat(holder)

	dfAMERICA = dfAMERICA.reset_index()

	return dfAMERICA

df = create_Full_List()

def 

print(df)

