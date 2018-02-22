import os
import pickle
import requests
import urllib.request
import datetime as dt
import csv
import json
import modules as mod
from streamingData import StreamingData
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import pandas_datareader.data as web
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy.orm import sessionmaker, scoped_session
import sys
sys.path.insert(0, '/home/epimetheus/Programming/Python/TradeKingApi')
import TradeKingAPI as api

def formatResponse(response):
    ##Content from the response format bytes
    holder1 = response.content

    ##Contest from the response converted from bytes to string
    holder2 = holder1.decode("utf-8")

    ##Loads string into json format dict
    result = json.loads(holder2)

    return result

def printJSON(jsonDict):
    prettyJSON = json.dumps(jsonDict, sort_keys=True, indent=4)

    print(prettyJSON)

def delAllTables(engine):
    dfListTables = pd.read_sql('show tables', engine)

    print(dfListTables)

    if dfListTables.empty:
        return None

    for tables in dfListTables.ix[:,0]:
        print(tables)

        if "Tables_in" in tables:
            continue 

        connection = engine.connect()
        result = connection.execute('DROP TABLE `{}`;'.format(tables))
        connection.close()

def indexDatetime(df):
    dfTest = df.index
    df.index = pd.to_datetime(dfTest)
    df.index.dtype

    return df

def engine(database, user='test_user', password='password', sqlServer = 'localhost'):
    if isinstance(sqlServer, str):
        sqlServer = sqlServer
    else:
        sqlServer = str(sqlServer)

    engine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format(user, password, sqlServer, database), echo=True, pool_recycle=3600)
    return engine

def addNoDuplicates(table, dfNew, engine):

     dfNew.to_sql('myTempTable', engine, if_exists ='replace')

     connection = engine.connect() 
     connection.execute('INSERT IGNORE INTO {} SELECT * FROM myTempTable;'.format(table))
     connection.close()

    
    
    






