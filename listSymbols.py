import os
import pickle
import pandas as pd
import modules as mod
import pymysql.cursors 

##List of Functions
#    listSymbols()
#    count()
#    list()

class ListSymbols():

    def __init__(self, sector='none', exchange='none', price=0, marketcap=0, IPOyear=0, max=0, min=0):
         self.sector = sector
         self.exchange = exchange
         self.price = price
         self.marketcap = marketcap
         self.IPOyear = IPOyear
         self.max = max
         self.min = min
         self.engine = mod.engine('Symbols', 'test_user', 'password')

    ##Method to load list of symbols
    def listSymbols(self):

         ##Loads symbol list from Database to Dataframe
         dfHolder = pd.read_sql_query("SELECT * FROM Symbols;", self.engine)
         del dfHolder['index']
     
         ##Dataframe of All Stocks
         if self.sector=='none' and self.exchange=='none' and self.price==0 and self.marketcap==0 and self.IPOyear==0:
              dfHolder = dfHolder

         ##Narrows Dataframe by criteria sector, exchange, etc ...
         else:
              if self.sector!='none':
                   dfHolder = dfHolder[dfHolder.Sector == self.sector]
          
              if self.exchange!='none':
                   dfHolder = dfHolder[dfHolder.Exchange == self.exchange]

              if self.price!=0:
                   if self.max==0 and self.min==0:
                        dfHolder = dfHolder
                   else:
                        if self.max==0:
                             dfHolder = dfHolder[dfHolder.LastSale >= self.min]
                        else:
                             dfHolder = dfHolder[dfHolder.LastSale <= self.max]
                             dfHolder = dfHolder[dfHolder.LastSale >= self.min]

              if self.marketcap!=0:
                   if self.max==0 and self.min==0:
                        dfHolder = dfHolder
                   else:
                        if self.max==0:
                             dfHolder = dfHolder[dfHolder.MarketCap >= self.min]
                        else:
                             dfHolder = dfHolder[dfHolder.MarketCap <= self.max]
                             dfHolder = dfHolder[dfHolder.MarketCap >= self.min]

         ##Loads Dataframe into pickle file
         with open("Stock_Data/Lists/listALL.pickle", "wb") as f:
              pickle.dump(dfHolder['Symbol'], f)

         with open("Stock_Data/Lists/listALL.pickle","rb") as f:
              tickers = pickle.load(f)
     
         return tickers;

    ##Used to test output of function
    def count(self):
         symbols = ListSymbols(self.sector, self.exchange, self.price, self.max, self.min).listSymbols()

         x=0

         for sym in symbols:
              x = x+1
         return x

    ##Used to print symbols
    def list(self):
         symbols = ListSymbols(self.sector, self.exchange, self.price, self.max, self.min).listSymbols()

         x=0

         for sym in symbols:
              print(sym)

##Test Runs
 
#test = ListSymbols(sector='Health Care', exchange='AMEX')

#test.list()
#print(test.count())



	
