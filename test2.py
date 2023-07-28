from time import tzname
from click import clear
import os
import datetime
from src.api.noren import NorenApiPy
from src.connectFlattrade import initialize
import dotenv
from src.logger import get_logger
from src.DataFetcher import FTDataService
from src.strategy.momentum import FilterStocks, BuyStocks
import pandas as pd

from src.utils.utils import get_epoch_time

dotenv.load_dotenv()
logger = get_logger(filename="./log")
today = datetime.date.today()
today_str = today.strftime("%d-%m-%Y")
api = initialize(today_str, logger)

strat_momentum_path = os.path.join('apidata', 'momentum', today_str)
os.makedirs(strat_momentum_path, exist_ok=True)

fs = FilterStocks(logger, api, df = pd.read_csv('./apidata/NSE/symbol_token.csv'), path=strat_momentum_path)
# fs.add_data(date=today)
# fs.filter_data()

ft_data = FTDataService(logger=logger, api=api, path='./apidata')
df_buy = pd.read_csv(os.path.join(strat_momentum_path, 'buy.csv'))
df_sell = pd.read_csv(os.path.join(strat_momentum_path, 'sell.csv'))
stt = []
for _, row in df_buy.iterrows():
    symbol, tsym, token = row['symbol'], row['tsym'], row['token']
    exch = "NSE"
    stt.append((symbol, tsym, token))
    ft_data.save_day(today, exchange=exch, symbol=symbol, tsym=tsym)