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
from src.api.noren import NorenApiPy, get_epoch_time
from src.EquityToken import FetchToken
from src.strategy.momentum import FilterStocks, BuyStocks
import datetime

dotenv.load_dotenv()
logger = get_logger(filename='./log')

# Init API
if True:
  con = ConnectFlatTrade(logger=logger)
  resp = con.run()
  con.set_token_to_dotenv()

token = os.getenv('TODAYSTOKEN')
api = NorenApiPy()
ret = api.set_session(userid='FT020770', password = '', usertoken=token)

# Get fno equity df
if False:
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

# Get top 10 losers and gainers F&O stock for trading (9:15-9:30 candle)

if False:
  fs = FilterStocks(logger, api, df_tsym)
  fs.add_data(date=datetime.date.today().strftime("%d-%m-%Y"))
  fs.filter_data()

# Buy and short stocks ~5lacs each at 9:35am
if True:
    call_df = pd.read_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/call_today.csv')
    put_df = pd.read_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/put_today.csv')
    bs = BuyStocks(logger, api, call_df, put_df)
    buy_df = bs.buy_init_stocks() 
    sell_df = bs.short_init_stocks()

from src.api.NorenApi import TimePriceParams, BuyorSell, ProductType, PriceType
# Monitor
buy_df = pd.read_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/equity_buy.csv')
sell_df = pd.read_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/equity_short.csv')
todaydate = datetime.date.today().strftime('%d-%m-%Y')
# tpp_list = [TimePriceParams("NSE", t['token'], get_epoch_time(todaydate+" 09:15:00"), get_epoch_time(todaydate+" 12:15:00"), 60) for _, t in buy_df.iterrows()]
# print(tpp_list)
# print(tpp_list)

# Buy options

options_df = pd.read_csv('https://shoonya.finvasia.com/NFO_symbols.txt.zip')

def get_trading_symbol(equity_symbol, expiry, call_put:bool, strike_price):
    ans = options_df[(options_df['Symbol'] == equity_symbol) &
                     (options_df['Expiry']== expiry) &
                     (options_df['StrikePrice'] == strike_price) &
                     (options_df['OptionType'] == 'CE' if call_put else 'PE') ]
    return ans['TradingSymbol'], ans['token'], ans['LotSize']

for _, t in buy_df.iterrows():
    strike_price = 60
    tsym, token, lotsize = get_trading_symbol(t['Symbol'], '27-07-2023', 1, strike_price)
    resp = api.place_order(buy_or_sell=BuyorSell.Buy, product_type=ProductType.Delivery,
                exchange='NFO', tradingsymbol=tsym, 
                quantity=lotsize, discloseqty=0,price_type=PriceType.Market, price=0, trigger_price=None,
                retention='DAY', remarks='')
    break