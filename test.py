import pandas as pd
import datetime
import time
import asyncio
import dotenv
import os
from dataclasses import dataclass
from src.logger import get_logger
from src.FnOList import FnOEquityNSE
from src.connectFlattrade import ConnectFlatTrade
from src.api.noren import NorenApiPy, get_time
from src.EquityToken import FetchToken
from src.strategy.momentum import FilterStocks, BuyStocks

dotenv.load_dotenv()
logger = get_logger(filename='./log')


# Init API
genToken = False
if genToken:
  con = ConnectFlatTrade(logger=logger)
  resp = con.run()
  con.set_token_to_dotenv()
token = os.getenv('TODAYSTOKEN')
# print(token)
api = NorenApiPy()
usersession=token
userid = 'FT020770'
ret = api.set_session(userid= userid, password = '', usertoken= usersession)

# Get fno equity df
is_present=True
if not is_present:
  fnoconnect = FnOEquityNSE(logger=logger, path='./nsedata/fo')
  df = fnoconnect.get_latest(update=True)
  def add_token():
    token_api = FetchToken(logger, api)
    tsym, token = token_api.get_tsym_token(df['UNDERLYING'])
    df['tsym'] = tsym
    df['token'] = token
    df.to_csv('./apidata/fno_equity_tsym_token.csv', index=False)
  add_token()

df_tsym = pd.read_csv('./apidata/fno_equity_tsym_token.csv')
# print(df_tsym.head())

# Get stock list
fs = FilterStocks(logger, api, df_tsym)

# '%d-%m-%Y %H:%M:%S'
# ans = api.get_time_price_series("NSE", 163, starttime=get_time("19-07-2023 09:15:00"), endtime=get_time("19-07-2023 09:30:00"), interval=15)
# print(ans)
# fs.add_data(starttime=get_time("19-07-2023 09:15:00"), endtime=get_time("19-07-2023 09:30:00"), interval=15)
# ans  = asyncio.run(fs.add_data(exchange="NSE", starttime=str(st), endtime=str(et), interval=str(3)))
fs.add_data()
fs.filter_data()

# Purchase the stocks ~5lacs at 9:35am
bs = BuyStocks(logger, api, pd.read_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/call_today.csv'))
bs.buy_init_stocks()

# Get options price