import os
import pickle
import datetime as dt
import csv
import json
import modules as mod
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import pandas_datareader.data as web
from listSymbols import ListSymbols
import sys
sys.path.insert(0, '/home/epimetheus/Programming/Python/TradeKingApi')
import TradeKingAPI as api

class StreamingData():

    def __init__(self, symbol):
        self.sym = symbol;
        self.parameters = {'format': 'json'};

    def stockQuotes(self):
        url = 'https://api.tradeking.com/v1/market/ext/quotes.json?symbols={}'.format(self.sym)

        response = api.authGET(url, self.parameters)

        result = mod.formatResponse(response)

        format1 = result['response']
        format2 = format1['quotes']
        resultFinal = format2['quote']

        dfResult = pd.io.json.json_normalize(resultFinal)

        return dfResult

    def stockOptions(self, expDate, optType, optStrike):

        expDate = str(expDate)
        optStrike = str(optStrike)        

        url = 'https://api.tradeking.com/v1/market/ext/quotes.json?symbols={}{}{}{}'.format(self.sym, expDate, optType, optStrike)

        response = api.authGET(url, self.parameters)

        result = api.formatResponse(response)

        format1 = result['response']
        format2 = format1['quotes']
        resultFinal = format2['quote']

        dfResult = pd.io.json.json_normalize(resultFinal)

        return dfResult

test = StreamingData('VMW')
print(test.stockOptions('180622', 'C', '00155000'))  
