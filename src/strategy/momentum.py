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

class FilterStocks:
    def __init__(self, logger, api:NorenApiPy, df:pd.DataFrame):
        self.df = df
        self.logger = logger
        self.api = api
    
    async def get_data(self, exchange, starttime, endtime, interval=1):
        # return self.api.get_time_price_series("NSE", "163", starttime, endtime, interval)
        tpp_list = [TimePriceParams(exchange, t['token'], starttime, endtime, interval) for _, t in self.df.iterrows()]
        # print(tpp_list)
        return await self.api.get_time_price_series_tpplist(tpp_list)
    
    def add_data(self):
        st = get_time("19-07-2023 09:30:00")
        et = get_time("19-07-2023 09:35:00") 
        raw_data = asyncio.run(self.get_data("NSE", st, et, 15))
        print(raw_data)

# fs = FilterStocks(get_logger(), pd.read_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/fno_equity_tsym_token.csv'))