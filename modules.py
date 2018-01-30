import os
import json
import flatten_json as fl
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float
from sqlalchemy.orm import sessionmaker, scoped_session
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import pandas_datareader.data as web

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

def engine(user, password, database, sqlServer = 'localhost'):
    if isinstance(sqlServer, str):
        sqlServer = sqlServer
    else:
        sqlServer = str(sqlServer)

    engine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format(user, password, sqlServer, database), echo=True, pool_recycle=3600)
    return engine





