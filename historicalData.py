import os
import datetime as dt
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.alphavantage import AlphaVantage
import pandas as pd
import pandas_datareader.data as web
import modules as mod
from listSymbols import ListSymbols
import end as end

##List of Functions
#    historicalMin()
#    historical15Min() 
#    historical30Min()
#    historicalHour()
#    historicalDaily() 
#    historicalDaily_Adjusted()

class HistoricalData():

    def __init__(self, database='stocks_test', user='test_user', password='password', sqlServer='localhost'):
        self.engine = mod.engine(database, user, password, sqlServer)
        self.ts = TimeSeries(key='XYI5EZUJ5CS5BI3E', output_format='pandas', retries=5)

    def historicalMin(self, tickers):
        skipped = 0;
        not_exist = False

        for ticker in tickers:
            end.end()

            if not self.engine.dialect.has_table(self.engine, ticker):
                not_exist = True

            try:     
                dfHolder, meta_data = self.ts.get_intraday(symbol='{}'.format(ticker),interval='1min', outputsize='full')

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
                skipped = skipped + 1
                pass

    def historical15Min(self, tickers):
        skipped = 0;
        not_exist = False

        for ticker in tickers:
            end.end()

            if not self.engine.dialect.has_table(self.engine, ticker):
                not_exist = True

            try:     
                dfHolder, meta_data = self.ts.get_intraday(symbol='{}'.format(ticker),interval='15min', outputsize='full')

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
                skipped = skipped + 1
                pass

        print("Finished")
        print("Amount Skipped - ", skipped)

    def historical30Min(self, tickers):
        skipped = 0;
        not_exist = False

        for ticker in tickers:
            end.end()

            if not self.engine.dialect.has_table(self.engine, ticker):
                not_exist = True

            try:     
                dfHolder, meta_data = self.ts.get_intraday(symbol='{}'.format(ticker),interval='30min', outputsize='full')

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
                skipped = skipped + 1
                pass

    def historicalHour(self, tickers):
        skipped = 0;
        not_exist = False

        for ticker in tickers:
            end.end()

            if not self.engine.dialect.has_table(self.engine, ticker):
                not_exist = True

            try:     
                dfHolder, meta_data = self.ts.get_intraday(symbol='{}'.format(ticker),interval='60min', outputsize='full')

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
                skipped = skipped + 1
                pass

    def historicalDaily(self, tickers, outputSize = 'compact'):
        skipped = 0;
        not_exist = False

        for ticker in tickers:
            end.end()

            if not self.engine.dialect.has_table(self.engine, ticker):
                not_exist = True

            try:     
                dfHolder, meta_data = self.ts.get_daily(symbol='{}'.format(ticker),interval='15min', outputsize='full')

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
                skipped = skipped + 1
                pass

    def historicalDailyAdjusted(self, tickers, outputSize = 'compact'):
        skipped = 0;
        not_exist = False

        for ticker in tickers:
            end.end()

            if not self.engine.dialect.has_table(self.engine, ticker):
                not_exist = True

            try:     
                dfHolder, meta_data = self.ts.get_daily_adjusted(symbol='{}'.format(ticker),interval='15min', outputsize='full')

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
                skipped = skipped + 1
                pass

##Test Runs
#symbols = ListSymbols(sector='Health Care', exchange='AMEX')
#tickers = symbols.listSymbols()
#count = symbols.count()

#HistoricalData(database='stocks_test_daily').historicalMin(tickers)
#HistoricalData().historical15Min(tickers)





 
