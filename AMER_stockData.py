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

def list_AMER():
	
	dfNYSE = pd.read_csv('Stock_Data/Lists/NYSE_stocks.csv')
	dfNASDAQ = pd.read_csv('Stock_Data/Lists/NASDAQ_stocks.csv')
	dfAMEX = pd.read_csv('Stock_Data/Lists/AMEX_stocks.csv')

	holder = [dfNYSE, dfNASDAQ, dfAMEX]

	dfAMERICA = pd.concat(holder)

	dfAMERICA = dfAMERICA.reset_index()

	del dfAMERICA['index']	

	return dfAMERICA

##df1 = list_AMER()

def list_AMER_Tech():

	dfAMER = list_AMER()
	
	dfAMER_Tech = pd.DataFrame()

	dfAMER_Tech = dfAMER

	for x in dfAMER.itertuples():
        ##print(x[7])
        	if x[7] != 'Technology':
                	dfAMER_Tech = dfAMER_Tech.drop(x[0])

	return dfAMER_Tech

def list_AMER_Fin():

        dfAMER = list_AMER()

        dfAMER_Fin = pd.DataFrame()

        dfAMER_Fin = dfAMER

        for x in dfAMER.itertuples():
        ##print(x[7])
                if x[7] != 'Finance':
                        dfAMER_Fin = dfAMER_Fin.drop(x[0])

        return dfAMER_Fin

df1 = list_AMER_Fin()
##df2 = list_AMER_Tech()		
print(df1)
##print(df2)

