import random
from time import tzname
from click import clear
import os
import datetime
from src.orders import OrderBook, Order, TranType
from src.connectFlattrade import initialize
import dotenv
from src.logger import get_logger
from src.DataFetcher import FTDataService
from src.strategy.momentum import FilterStocks, BuyStocks
import pandas as pd

from src.utils.utils import get_epoch_time

dotenv.load_dotenv()
logger = get_logger(filename="./log")
today = datetime.date.today() - datetime.timedelta(days=1)
today_str = today.strftime("%d-%m-%Y")
api = initialize(today_str, logger)

strat_momentum_path = os.path.join('apidata', 'momentum', today_str)
os.makedirs(strat_momentum_path, exist_ok=True)

ft_data = FTDataService(logger=logger, api=api, path='./apidata')

# Strategy begins

# Step 1
# Filter the list of top 10 gainers and losers
fs = FilterStocks(logger, api, df = pd.read_csv('./apidata/NSE/symbol_token.csv'), path=strat_momentum_path)
fs.add_data(date=today)
fs.filter_data()
df_buy = pd.read_csv(os.path.join(strat_momentum_path, 'buy.csv'))
df_sell = pd.read_csv(os.path.join(strat_momentum_path, 'sell.csv'))

BALANCE = 50_00_000
EQ_PER_SHARE = 1_00_000
ob = OrderBook(BALANCE)
# Buy Gainers, sell losers @ 9:30am (equity MIS)
ordtime = datetime.datetime.strptime(today_str+" 09:31:00", "%d-%m-%Y %H:%M:%S")
exch = "NSE"
for _, row in df_buy.iterrows():
    symbol, tsym, token = row['symbol'], row['tsym'], row['token']
    ft_data.save_day(today, exchange=exch, symbol=symbol, tsym=tsym)
    avgprc = ft_data.get_quote(ordtime, exch, symbol, tsym)
    qty = int(EQ_PER_SHARE/avgprc)
    norenordernum = random.randrange(1_000_000, 10_000_000)
    ord = Order(norenordernum, ordtime=ordtime, sym=symbol, tsym=tsym, token=token, ls=1, avgprice=avgprc, qty=qty, tranType=TranType.Buy)
    if ob.add(ord):
        logger.info("Order placed %s, %s, %s", ordtime, tsym, avgprc)
    else:
        logger.error("Unable to place Order  %s, %s, %s", ordtime, tsym, avgprc)

# for _, row in df_sell.iterrows():
#     symbol, tsym, token = row['symbol'], row['tsym'], row['token']
#     ft_data.save_day(today, exchange=exch, symbol=symbol, tsym=tsym)
#     avgprc = ft_data.get_quote(ordtime, exch, symbol, tsym)
#     qty = int(EQ_PER_SHARE/avgprc)
#     norenordernum = random.randrange(1_000_000, 10_000_000)
#     ord = Order(norenordernum, ordtime=ordtime, sym=symbol, tsym=tsym, token=token, ls=1, avgprice=avgprc, qty=qty, tranType=TranType.Buy)
#     if ob.add(ord):
#         logger.info("Order placed %s, %s, %s", ordtime, tsym, avgprc)

# Step 2
# Check for avg hourly gains and buy call options
st = get_epoch_time(f"{today_str} 09:15:00")
et = get_epoch_time(f"{today_str} 12:14:00")
def step1(st, et):
    for _, row in df_buy.iterrows():
        symbol, tsym, token = row['symbol'], row['tsym'], row['token']
        hrly_df = ft_data.get_time_price_series(exch, symbol, tsym, st, et, 60)
        hrly_df['change'] = hrly_df['intc'] - hrly_df['into']
        hrly_df['perc'] = (hrly_df['change'] / hrly_df['intc']) * 100
        mean_perc = hrly_df.perc.mean()
        if (mean_perc > 0.2):
            # Place order for call options
            # Find tsym
            # Buy call options
            # Margin is the total price only, no worries in buying call options
            # avgprc = ft_data.get_quote(ordtime, exch, symbol, tsym)
            # qty = int(EQ_PER_SHARE/avgprc)
            pass

# Step 3
# Exit both stocks and options when current price is < 0.2% of peak today
def step3(t):
    for _, row in df_buy.iterrows():
        symbol, tsym, token = row['symbol'], row['tsym'], row['token']
        quote = ft_data.get_quote(t, exch, symbol, tsym, st)
        prev_peak = ft_data.get_prev_peak(t, exch, symbol, tsym, st)
        perc = ((quote-prev_peak)/prev_peak) * 100
        if (perc > 0.2):
            # Place order to sell call options and stocks by looking into the order book
            # Find tsym, qty from order book
            # Sell call options
            # avgprc = ft_data.get_quote(ordtime, exch, symbol, tsym)
            pass

#Step 4
# Call step3 in 2 mins interval (if there remains any order to sell)
# Call step2 hourly
# Till the market closes


    