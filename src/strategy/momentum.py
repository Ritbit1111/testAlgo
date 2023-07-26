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
sys.path.insert(0, '/Users/nbrk/AlgoTrade/testAlgo/')
from src.logger import get_logger
import asyncio
from src.api.noren import NorenApiPy, get_epoch_time
import tqdm

class FilterStocks:
    def __init__(self, logger, api:NorenApiPy, df:pd.DataFrame, threshold_call=0.2, threshold_put=-0.2):
        self.df = df
        self.logger = logger
        self.api = api
        self.thput=threshold_put
        self.thcall=threshold_call
    
    async def get_data(self, exchange, starttime, endtime, interval=1):
        # return self.api.get_time_price_series("NSE", "163", starttime, endtime, interval)
        tpp_list = [TimePriceParams(exchange, t['token'], starttime, endtime, interval) for _, t in self.df.iterrows()]
        # print(tpp_list)
        return await self.api.get_time_price_series_tpplist(tpp_list)
    
    def add_data(self, date):
        st = get_epoch_time(date+" 09:15:00")
        et = get_epoch_time(date+" 09:30:00") 
        raw_data = asyncio.run(self.get_data("NSE", st, et, 15))
        df_markte_open = pd.DataFrame([tdata[0] for tdata in raw_data])
        df_new = pd.concat([self.df, df_markte_open], axis=1)
        df_new.to_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/fno_equity_tsym_token_first15.csv', index=False)
    
    def filter_data(self):
        df_new = pd.read_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/fno_equity_tsym_token_first15.csv')
        df_new['change'] = df_new['intc'] - df_new['into']
        df_new['chperc'] = (df_new['change']/df_new['into'])*100
        df = df_new.sort_values(by='chperc')
        df_call = df.tail(10).sort_values(by='chperc', ascending=False)
        df_put = df.head(10)
        df_call = df_call[df_call['chperc']>self.thcall]
        df_put = df_put[df_put['chperc']<self.thput]
        df_call.to_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/call_today.csv')
        df_put.to_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/put_today.csv')
    
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
        # self.call_df.to_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/equity_buy.csv', index=False)
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