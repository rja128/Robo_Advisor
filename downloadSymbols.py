import os
import pickle
import requests
import urllib.request
import csv
import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy.orm import sessionmaker, scoped_session
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators 
import end as end
import modules as mod
from listSymbols import ListSymbols


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
    engine = mod.engine('Symbols', 'test_user', 'password', 'localhost')

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

def alphaVantage(tickers=ListSymbols().listSymbols()):
    ts = TimeSeries(key='XYI5EZUJ5CS5BI3E', output_format='pandas', retries=5)
    engine = mod.engine('Symbols', 'test_user', 'password', 'localhost')

    listTickers = []
    count = 0;
    for tick in tickers:
        end.end()
        count = count + 1
        listTickers.append(tick)

        if count == 100:
            time.sleep(10)

            try:
                dfHolder, meta_data = ts.get_batch_stock_quotes(listTickers)

                dfSymbols = dfHolder['1. symbol']

                if counter == 0:
                    dfSymbols.to_sql('AlphaSymbols', engine, if_exists='replace')
                else:
                    dfSymbols.to_sql('AlphaSymbols', engine, if_exists='append')

            except:
                print("except")
                pass
  
            count = 0;
            listTickers = []
            print(dfSymbols)

alphaVantage()	


