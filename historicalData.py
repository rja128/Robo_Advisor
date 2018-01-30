import os
import datetime as dt
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.alphavantage import AlphaVantage
import pandas as pd
import pandas_datareader.data as web
from listSymbols import ListSymbols
import modules as mod
import end as end

##List of Functions
#    historicalMin()
#    historical15Min() 
#    historical30Min()
#    historicalHour()
#    historicalDaily() 
#    historicalDaily_Adjusted()

class HistoricalData():

    def __init__(self, user='test_user', password='password', sqlServer='localhost', database='stocks_test'):
        self.engine = mod.engine(user, password, database, sqlServer)
        self.ts = TimeSeries(key='XYI5EZUJ5CS5BI3E', output_format='pandas', retries=5)

    def historicalMin(self, tickers):
        skipped = 0;

        for ticker in tickers:
            end.end()
            try:     
                dfHolder, meta_data = self.ts.get_intraday(symbol='{}'.format(ticker),interval='1min', outputsize='full')

                dfTest = dfHolder.index
                dfHolder.index = pd.to_datetime(dfTest)
                dfHolder.index.dtype
                dfHolder.to_sql('{}'.format(ticker), self.engine, if_exists='append')
                end.end()
            except:
                print("Skipped {}".format(ticker))
                skipped = skipped + 1
                pass

        print("Finished")
        print("Amount Skipped - ", skipped)

    def historical15Min(self, tickers):
        skipped = 0;

        for ticker in tickers:
            end.end()
            try:     
                dfHolder, meta_data = self.ts.get_intraday(symbol='{}'.format(ticker),interval='15min', outputsize='full')

                dfTest = dfHolder.index
                dfHolder.index = pd.to_datetime(dfTest)
                dfHolder.index.dtype
                dfHolder.to_sql('{}'.format(ticker), self.engine, if_exists='append')
            except:
                print("Skipped {}".format(ticker))
                skipped = skipped + 1
                pass

        print("Finished")
        print("Amount Skipped - ", skipped)

    def historical30Min(self, tickers):
        skipped = 0;

        for ticker in tickers:
            end.end()
            try:     
                dfHolder, meta_data = self.ts.get_intraday(symbol='{}'.format(ticker),interval='30min', outputsize='full')

                dfTest = dfHolder.index
                dfHolder.index = pd.to_datetime(dfTest)
                dfHolder.index.dtype
                dfHolder.to_sql('{}'.format(ticker), self.engine, if_exists='append')
            except:
                print("Skipped {}".format(ticker))
                skipped = skipped + 1
                pass

        print("Finished")
        print("Amount Skipped - ", skipped)

    def historicalHour(self, tickers):
        skipped = 0;

        for ticker in tickers:
            end.end()
            try:     
                dfHolder, meta_data = self.ts.get_intraday(symbol='{}'.format(ticker),interval='60min', outputsize='full')

                dfTest = dfHolder.index
                dfHolder.index = pd.to_datetime(dfTest)
                dfHolder.index.dtype
                dfHolder.to_sql('{}'.format(ticker), self.engine, if_exists='append', chunksize=15)
            except:
                print("Skipped {}".format(ticker))
                skipped = skipped + 1
                pass

        print("Finished")
        print("Amount Skipped - ", skipped)

    def historicalDaily(self, tickers, outputSize = 'compact'):
        skipped = 0;

        for ticker in tickers:
            end.end()
            try:     
                dfHolder, meta_data = self.ts.get_daily(symbol='{}'.format(ticker), outputsize=outputSize)

                dfTest = dfHolder.index
                dfHolder.index = pd.to_datetime(dfTest)
                dfHolder.index.dtype
                dfHolder.to_sql('{}'.format(ticker), self.engine, if_exists='append', chunksize=15)
            except:
                print("Skipped {}".format(ticker))
                skipped = skipped + 1
                pass

        print("Finished")
        print("Amount Skipped - ", skipped)

    def historicalDailyAdjusted(self, tickers, outputSize = 'compact'):
        skipped = 0;

        for ticker in tickers:
            end.end()
            try:     
                dfHolder, meta_data = self.ts.get_daily_adjusted(symbol='{}'.format(ticker), outputsize=outputSize)

                dfTest = dfHolder.index
                dfHolder.index = pd.to_datetime(dfTest)
                dfHolder.index.dtype
                dfHolder.to_sql('{}'.format(ticker), self.engine, if_exists='append', chunksize=15)
            except:
                print("Skipped {}".format(ticker))
                skipped = skipped + 1
                pass

        print("Finished")
        print("Amount Skipped - ", skipped)

##Test Runs
symbols = ListSymbols(sector='Health Care', exchange='AMEX')
tickers = symbols.listSymbols()
count = symbols.count()

HistoricalData(database='stocks_test_daily').historicalDaily(tickers)
#HistoricalData().historical30Min(tickers)





 
