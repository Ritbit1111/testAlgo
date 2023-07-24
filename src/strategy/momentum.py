# Find top 10 gainers and losers in 9:15 to 9:30
'''
Input:  Time interval
        Stocks list
Output: Gainers, Losers
'''
import pandas as pd
import sys
from src.api.noren import NorenApiPy
from src.api.NorenApi import TimePriceParams
sys.path.insert(0, '/Users/nbrk/AlgoTrade/testAlgo/')
from src.logger import get_logger
import asyncio
from src.api.noren import NorenApiPy, get_time
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
    
    def add_data(self):
        st = get_time("24-07-2023 09:15:00")
        et = get_time("24-07-2023 09:30:00") 
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
    def __init__(self, logger, api, call_df) -> None:
        self.df = call_df
        self.logger = logger
        self.api = api
    
    def buy_init_stocks(self):
        self.df = self.df.head(5)
        book_price = []
        quantity = []
        for index, row in self.df.iterrows():
            bp, qty = self.get_book_price(row['token'])
            print(bp, qty)
            book_price.append(bp)
            quantity.append(qty)
        self.df['quantity'] = quantity
        self.df['book_price'] = book_price
        self.df.to_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/call_today_booked.csv')
    
    def get_book_price(self, token):
        data = self.api.get_time_price_series("NSE", token, get_time("24-07-2023 09:35:00"), get_time("24-07-2023 09:36:00"), 1)
        buy_price = data[0]["intc"]
        qty = int(self.PER_STOCK_PRICE/float(buy_price))
        print(buy_price, qty)
        return buy_price, qty