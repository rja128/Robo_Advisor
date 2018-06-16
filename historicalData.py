import os
import datetime as dt
import pickle
import time
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.alphavantage import AlphaVantage
import pandas as pd
import pandas_datareader.data as web
import modules as mod
from listSymbols import ListSymbols
import end as end

##List of Functions
#    historicalMin()
#    historicalHour()
#    historicalDaily() 
#    historicalDaily_Adjusted()

class HistoricalData():

    def __init__(self, database='stocks_test', user='test_user', password='password', sqlServer='localhost', engine=None):
        if engine != None:
            self.engine = engine
        else:
            self.engine = mod.engine(database, user, password, sqlServer)

        self.ts = TimeSeries(key='XYI5EZUJ5CS5BI3E', output_format='pandas', retries=5)  
        self.prevSkipped = []      

    def historicalIntraday(self, tickers, timeInterval=1):
        listSkipped = []
        skipped = 0;
        not_exist = False

        for ticker in tickers:
            not_exist = False
            end.end()
            time.sleep(3)

            try:
                if not self.engine.dialect.has_table(self.engine, ticker):
                    print("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
                    not_exist = True
    
                dfHolder, meta_data = self.ts.get_intraday(symbol='{}'.format(ticker),interval='{}min'.format(timeInterval), outputsize='full')

                dfHolder = mod.indexDatetime(dfHolder)

                if not_exist:
                    print("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                    dfHolder.to_sql('{}'.format(ticker), self.engine, if_exists ='fail')

                    connection = self.engine.connect() 
                    result = connection.execute('ALTER TABLE {} ADD PRIMARY KEY (date);'.format(ticker))
                    connection.close()

                else:
                    print("GGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG")
                    mod.addNoDuplicates(ticker, dfHolder, self.engine)

            except:
                print("Skipped {}".format(ticker))
                listSkipped.append(ticker)
                skipped = skipped + 1
                pass

        while not self.prevSkipped == listSkipped:
            self.prevSkipped = listSkipped

            self.historicalMin(listSkipped)

        self.prevSkipped == []
        print("Finished")

    def historicalDaily(self, tickers, outputSize = 'compact'):
        listSkipped = []
        skipped = 0;
        not_exist = False

        for ticker in tickers:
            not_exist = False
            end.end()
            print("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
            time.sleep(3)

            try:
                if not self.engine.dialect.has_table(self.engine, ticker):
                    not_exist = True
    
                dfHolder, meta_data = self.ts.get_daily(symbol='{}'.format(ticker), outputsize=outputSize)

                dfHolder = mod.indexDatetime(dfHolder)

                if not_exist:
                    dfHolder.to_sql('{}'.format(ticker), self.engine, if_exists ='fail')

                    connection = self.engine.connect() 
                    result = connection.execute('ALTER TABLE {} ADD PRIMARY KEY (date);'.format(ticker))
                    connection.close()

                else:
                    mod.addNoDuplicates(ticker, dfHolder, self.engine)

            except:
                print("Skipped {}".format(ticker))
                listSkipped.append(ticker)
                skipped = skipped + 1
                pass

        while not self.prevSkipped == listSkipped:
            self.prevSkipped = listSkipped

            self.historicalDaily(listSkipped)

        self.prevSkipped == []
        print("Finished")

    def historicalDailyAdjusted(self, tickers, outputSize = 'compact'):
        listSkipped = []
        skipped = 0;
        not_exist = False

        for ticker in tickers:
            not_exist = False
            end.end()
            time.sleep(3)

            try:
                if not self.engine.dialect.has_table(self.engine, ticker):
                    not_exist = True
     
                dfHolder, meta_data = self.ts.get_daily_adjusted(symbol='{}'.format(ticker), outputsize=outputSize)

                dfHolder = mod.indexDatetime(dfHolder)

                if not_exist:
                    dfHolder.to_sql('{}'.format(ticker), self.engine, if_exists ='fail')

                    connection = self.engine.connect() 
                    result = connection.execute('ALTER TABLE {} ADD PRIMARY KEY (date);'.format(ticker))
                    connection.close()

                else:
                    mod.addNoDuplicates(ticker, dfHolder, self.engine)

            except:
                print("Skipped {}".format(ticker))
                listSkipped.append(ticker)
                skipped = skipped + 1
                pass

        while not self.prevSkipped == listSkipped:
            self.prevSkipped = listSkipped

            self.historicalDailyAdjusted(listSkipped)

        self.prevSkipped == []
        print("Finished")

##Test Runs
#symbols = ListSymbols(sector='Health Care', exchange='AMEX')
#tickers = symbols.listSymbols()
#count = symbols.count()

#HistoricalData(database='stocks_test_daily').historicalMin(tickers)
#HistoricalData().historical15Min(tickers)





 
