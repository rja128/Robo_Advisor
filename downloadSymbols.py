import os
import pickle
import requests
import urllib.request
import csv
import pandas as pd
from sqlalchemy import create_engine
import pymysql.cursors

urlNASDAQ = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download'
urlNYSE = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NYSE&render=download'
urlAMEX = 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download'

##Download CSV of list of stocks
def symbolCSVs():
    os.remove("Stock_Data/Lists/NASDAQ_stocks.csv")
    os.remove("Stock_Data/Lists/AMEX_stocks.csv")
    os.remove("Stock_Data/Lists/NYSE_stocks.csv")

    csvNASDAQ = urllib.request.urlopen(urlNASDAQ).read() # returns type 'bytes'
    with open('Stock_Data/Lists/NASDAQ_stocks.csv', 'wb') as fx: # bytes, hence mode 'wb'
         fx.write(csvNASDAQ)

    csvNYSE = urllib.request.urlopen(urlNYSE).read() # returns type 'bytes'
    with open('Stock_Data/Lists/NYSE_stocks.csv', 'wb') as fx: # bytes, hence mode 'wb'
         fx.write(csvNYSE)

    csvAMEX = urllib.request.urlopen(urlAMEX).read() # returns type 'bytes'
    with open('Stock_Data/Lists/AMEX_stocks.csv', 'wb') as fx: # bytes, hence mode 'wb'
        fx.write(csvAMEX)

##Create Database of Symbols
def symbols_to_Database():
    symbolCSVs()
     
    ##Connect to MYSQL Database
    engine = create_engine('mysql+pymysql://test_user:password@localhost/stocks_test', echo=True, pool_recycle=3600)

    ##Save CSVs to Dataframes
    dfNYSE = pd.read_csv('Stock_Data/Lists/NYSE_stocks.csv')
    dfNASDAQ = pd.read_csv('Stock_Data/Lists/NASDAQ_stocks.csv')
    dfAMEX = pd.read_csv('Stock_Data/Lists/AMEX_stocks.csv')

    ##Add Exchange data to dataframe
    dfAMEX['Exchange'] = 'AMEX'
    dfNYSE['Exchange'] = 'NYSE'
    dfNASDAQ['Exchange'] = 'NASDAQ'

    ##Combine Dataframes for one big list
    holder = [dfNYSE, dfNASDAQ, dfAMEX]
    dfAMERICA = pd.concat(holder)

    ##Modify complete stock list Dataframe so it can be imported to MYSQL
    dfAMERICA = dfAMERICA.reset_index()
    del dfAMERICA['index']
    del dfAMERICA['ADR TSO']
    del dfAMERICA['Unnamed: 9']
    del dfAMERICA['Summary Quote']

    ##Convert LastSale type to Integers by first replacing n/a with "" (empty)
    dfAMERICA.LastSale[dfAMERICA.LastSale=='n/a'] = ""
    print(dfAMERICA)
    dfAMERICA['LastSale'] = pd.to_numeric(dfAMERICA.LastSale)

    ##Upload List to MYSQL Database
    dfAMERICA.to_sql('Symbols', engine, if_exists='replace')

    return 	


