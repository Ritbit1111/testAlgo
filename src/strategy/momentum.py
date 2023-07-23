# Find top 10 gainers and losers in 9:15 to 9:30
'''
Input:  Time interval
        Stocks list
Output: Gainers, Losers
'''
import pandas as pd
import sys
from src.api.noren import NorenApiPy
sys.path.insert(0, '/Users/nbrk/AlgoTrade/testAlgo/')
from src.logger import get_logger
import asyncio

class FilterStocks:
    def __init__(self, logger, api:NorenApiPy, df:pd.DataFrame):
        self.df = df
        self.logger = logger
        self.api = api
    
    async def fetch_time_price(self, exchange, token, starttime, endtime, interval):
        return await asyncio.to_thread(self.api.get_time_price_series, exchange, token, starttime, endtime, interval)

    async def add_data(self, starttime, endtime, interval=1):
        datali = []
        for index, row in self.df.iterrows():
            print('NSE', str(row['token']), starttime, endtime, interval)
            datali.append(self.fetch_time_price('NSE', str(row['token']), starttime, endtime, interval))
        output = await asyncio.gather(*datali)
        return output


# fs = FilterStocks(get_logger(), pd.read_csv('/Users/nbrk/AlgoTrade/testAlgo/apidata/fno_equity_tsym_token.csv'))