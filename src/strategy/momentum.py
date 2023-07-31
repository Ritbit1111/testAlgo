# Find top 10 gainers and losers in 9:15 to 9:30
'''
Input:  Time interval
        Stocks list
Output: Gainers, Losers
'''
import pandas as pd
import sys
from src.api.noren import NorenApiPy
from src.api.NorenApi import TimePriceParams, BuyorSell, ProductType, PriceType
from src.logger import get_logger
import asyncio
from src.utils.utils import epochIndian, get_epoch_time
import os
import datetime
from src.DataFetcher import FTDataService, OptionType

class FilterStocks:
    def __init__(self, logger, api:NorenApiPy, df:pd.DataFrame, date:datetime.datetime, path, threshold_call=0.2, threshold_put=-0.2):
        self.df = df
        self.logger = logger
        self.api = api
        self.date = date
        self.thput=threshold_put
        self.thcall=threshold_call
        self.path = path

    
    async def get_data(self, exchange, starttime, endtime, interval=1):
        tpp_list = []
        for _, t in self.df.iterrows():
            tpp_list.append(TimePriceParams(exchange, t['token'], starttime, endtime, interval))
        return await self.api.get_time_price_series_tpplist(tpp_list)
    
    def add_data(self):
        mopath = os.path.join(self.path, 'market_open.csv')
        # if os.path.exists(mopath):
        #     return
        st = epochIndian(self.date.replace(hour=9, minute=15, second=0))
        et = epochIndian(self.date.replace(hour=9, minute=30, second=0))
        raw_data = asyncio.run(self.get_data("NSE", st, et, 15))
        df_markte_open = pd.DataFrame([tdata[0] for tdata in raw_data])
        df_market_open = pd.concat([self.df, df_markte_open], axis=1)
        self.df_market_open = df_market_open
        self.df_market_open.to_csv(mopath, index=None)
    
    def filter_data(self):
        bpath = os.path.join(self.path, 'buy.csv')
        spath = os.path.join(self.path, 'sell.csv')
        mopath = os.path.join(self.path, 'market_open.csv')
        # if os.path.exists(bpath) and os.path.exists(spath):
        #     return
        df_market_open = pd.read_csv(mopath)
        df_market_open['change'] = df_market_open['intc'] - df_market_open['into']
        df_market_open['chperc'] = (df_market_open['change']/df_market_open['into'])*100
        df = df_market_open.sort_values(by='chperc')
        df_call = df.tail(10).sort_values(by='chperc', ascending=False)
        df_put = df.head(10)
        df_call = df_call[df_call['chperc']>self.thcall]
        df_put = df_put[df_put['chperc']<self.thput]
        df_call.to_csv(bpath, index=None)
        df_put.to_csv(spath, index=None)
    
    
# fs = FilterStocks(get_logger(), pd.read_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/fno_equity_tsym_token.csv'))

class BuyStocks:
    PER_STOCK_PRICE = 100000
    def __init__(self, logger, api:NorenApiPy, call_df, put_df) -> None:
        self.call_df = call_df
        self.put_df = put_df
        self.logger = logger
        self.api = api
    
    def buy_init_stocks(self):
        self.call_df = self.call_df[:5]
        book_price = []
        quantity = []
        for index, row in self.call_df.iterrows():
            bp, qty = self.get_book_price(row['tsym'], row['token'])
            # print(bp, qty)
            book_price.append(bp)
            quantity.append(qty)
        self.call_df['quantity'] = quantity
        self.call_df['book_price'] = book_price
        self.call_df.to_csv('apidata/equity_buy.csv', index=False)
        return self.call_df
    
    def short_init_stocks(self):
        self.put_df = self.put_df[:5]
        book_price = []
        quantity = []
        for index, row in self.put_df.iterrows():
            bp, qty = self.get_book_price(row['tsym'], row['token'])
            # print(bp, qty)
            book_price.append(bp)
            quantity.append(qty)
        self.put_df['quantity'] = quantity
        self.put_df['book_price'] = book_price
        self.put_df.to_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/equity_short.csv', index=False)
        return self.put_df

    def get_book_price(self, tsym, token):
        data = self.api.get_time_price_series("NSE", token, get_epoch_time("24-07-2023 09:35:00"), get_epoch_time("24-07-2023 09:36:00"), 1)
        # If simulating real time, then get data from self.api.get_quote("NSE", token) response["lp"]
        buy_price = data[0]["intc"]
        qty = int(self.PER_STOCK_PRICE/float(buy_price))
        # If deployed real time, then directly place Market order
        # resp = self.api.place_order(buy_or_sell=BuyorSell.Buy, product_type=ProductType.Delivery,
        #         exchange='NSE', tradingsymbol=tsym, 
        #         quantity=qty, discloseqty=0,price_type=PriceType.Market, price=0, trigger_price=None,
        #         retention='DAY', remarks='my_order_001')
        # Read the noren number from the resp and use self.api.single_order_history(orderno), res['avgprc'], res['fillshares']
        # gives traded price and qty
        # print(buy_price, qty)
        return buy_price, qty