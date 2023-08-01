import random
from time import tzname
from click import clear
import os
import datetime
from src.orders import Instrument, OrderBook, Order, TranType
from src.connectFlattrade import initialize
import dotenv
from src.logger import get_logger
from src.DataFetcher import FTDataService, OptionType
from src.strategy.momentum import FilterStocks, BuyStocks
import pandas as pd

from src.utils.utils import get_epoch_time

dotenv.load_dotenv()
logger = get_logger(filename="./log")
today = datetime.datetime(2023, 7, 28)
today_str = today.strftime("%d-%m-%Y")
api = initialize(today_str, logger)

strat_momentum_path = os.path.join("apidata", "momentum", today_str)
os.makedirs(strat_momentum_path, exist_ok=True)

ft_data = FTDataService(logger=logger, api=api, path="./apidata")

activeFnOsymbols = ft_data.active_FnO_symbol_list(today)
df = pd.read_csv("./apidata/NSE/symbol_token.csv")
delrows = []
for index, row in df.iterrows():
    if row['symbol'] not in activeFnOsymbols:
        delrows.append(index)
df = df.drop(delrows, axis=0).reset_index(drop=True)

# Strategy begins

# Step 1
# Filter the list of top 10 gainers and losers
fs = FilterStocks(
    logger,
    api,
    date=today,
    df=df,
    path=strat_momentum_path,
)
fs.add_data()
fs.filter_data()

EQUITY_BUY_COUNT=5
EQUITY_SELL_COUNT=5
df_buy  = pd.read_csv(os.path.join(strat_momentum_path, "buy.csv"), nrows=EQUITY_BUY_COUNT)
df_sell = pd.read_csv(os.path.join(strat_momentum_path, "sell.csv"), nrows=EQUITY_SELL_COUNT)

BALANCE = 50_00_000
BALANCE_PER_SHARE = 1_00_000
MARGIN_PER_OPT = 1_00_000
ob = OrderBook(BALANCE)
# Buy Gainers, sell losers @ 9:30am (equity MIS)
ordtime = datetime.datetime.strptime(today_str + " 09:31:00", "%d-%m-%Y %H:%M:%S")
for idx, df in enumerate([df_buy, df_sell]):
    exch = "NSE"
    for _, row in df.iterrows():
        symbol, tsym, token = row["symbol"], row["tsym"], row["token"]
        ft_data.save_day(today, exchange=exch, symbol=symbol, tsym=tsym)
        avgprc = ft_data.get_quote(ordtime, exch, symbol, tsym)
        qty = int(BALANCE_PER_SHARE / avgprc)
        norenordernum = random.randrange(1_000_000, 10_000_000)
        trantype = TranType.Buy if idx == 0 else TranType.Sell
        ord = Order(
            norenordernum,
            ordtime=ordtime,
            sym=symbol,
            tsym=tsym,
            token=token,
            instrument=Instrument.Equity,
            ls=1,
            avgprice=avgprc,
            qty=qty,
            tranType=trantype
        )
        if ob.add(ord):
            logger.info("Order placed %s, %s, %s", ordtime, tsym, avgprc)
        else:
            logger.error("Unable to place Order  %s, %s, %s", ordtime, tsym, avgprc)
print(ob)
# Step 2
# Check for avg hourly gains and buy call options
OPT_BUY_THRESHOLD = 0.2
OPT_SELL_THRESHOLD = 0.2

def step2_buyoptions(ordtime):
    print("Starting Step 2")
    st = ordtime - datetime.timedelta(hours=3)
    for idx, df in enumerate([df_buy, df_sell]):
        for _, row in df.iterrows():
            symbol, tsym, token = row["symbol"], row["tsym"], row["token"]
            hrly_df = ft_data.get_time_price_series(exch, symbol, tsym, st, ordtime, 60)
            hrly_df["change"] = hrly_df["intc"] - hrly_df["into"]
            hrly_df["perc"] = (hrly_df["change"] / hrly_df["intc"]) * 100
            mean_perc = hrly_df.perc.mean()
            call_put = 1 if idx==0 else -1
            buy_condition = (abs(mean_perc) > OPT_BUY_THRESHOLD) and ( mean_perc * call_put) > 0
            if buy_condition:
                qp = ft_data.get_quote(ordtime, "NSE", symbol, tsym)
                if qp is  None:
                    continue
                opt_type = OptionType.CALL if call_put == 1 else OptionType.PUT
                nfo_data = ft_data.get_closest_option_scrip( symbol=symbol, expiry=ordtime, quoteprice=qp, option_type=opt_type,)
                tsym_opt, token_opt, ls, sp = ( nfo_data["tsym"], nfo_data["token"], nfo_data["lotsize"], nfo_data["strikeprice"],)

                ft_data.save_day(today, exchange="NFO", symbol=symbol, tsym=tsym_opt)
                avgprc = ft_data.get_quote(ordtime=ordtime, exchange="NFO", symbol=symbol, tsym=tsym_opt)
                if avgprc is None:
                    continue
                qty = int(MARGIN_PER_OPT / avgprc)
                qty = (qty // ls) * ls
                norenordernum = random.randrange(1_000_000, 10_000_000)
                ord = Order( norenordernum, ordtime=ordtime, sym=symbol, tsym=tsym_opt,
                    token=token_opt,
                    instrument=Instrument.OptionsStock,
                    ls=ls,
                    avgprice=avgprc,
                    qty=qty,
                    tranType=TranType.Buy,
                )
                if ob.add(ord):
                    logger.info("Order placed %s, %s, %s", ordtime, tsym, avgprc)
                else:
                    logger.error("Unable to place Order  %s, %s, %s", ordtime, tsym, avgprc)


# Step 3
# Exit both stocks and options when current price is < 0.2% of peak today
def step3_exit(t, force_exit=False):
    print("Starting Step 3")
    for idx, df in enumerate([df_buy, df_sell]):
        for _, row in df.iterrows():
            symbol, tsym, token = row["symbol"], row["tsym"], row["token"]
            quote = ft_data.get_quote(t, exch, symbol, tsym)
            if quote is None:
                continue
            prev_peak = ft_data.get_prev_peak(t, exch, symbol, tsym)
            if prev_peak is None:
                continue
            perc = ((quote - prev_peak) / prev_peak) * 100
            call_put = 1 if idx==0 else -1
            if force_exit:
                exit_condition = True
            else:
                exit_condition = (abs(perc) > OPT_SELL_THRESHOLD) and (perc * call_put) < 0
            if exit_condition:
                # Equities : Sell purchased equity and buy shorted ones
                qty = ob.active_equity_qty(symbol)
                if qty!=0:
                    avgprc = ft_data.get_quote(t, "NSE", symbol, tsym)
                    if avgprc:
                        norenordernum = random.randrange(1_000_000, 10_000_000)
                        trantype = TranType.Buy if qty<0 else TranType.Sell
                        ord = Order(
                            norenordernum,
                            ordtime=ordtime,
                            sym=symbol,
                            tsym=tsym,
                            token=token,
                            instrument=Instrument.Equity,
                            ls=1,
                            avgprice=avgprc,
                            qty=qty,
                            tranType=trantype
                        )
                        ob.add(ord)
                
                # Place order to sell call and put options
                # Find tsym, qty from order book
                tsym_qty = ob.active_fno(symbol)
                for tsym_opt, qty in tsym_qty.items():
                    nfo_data = ft_data.get_nfo_info(tsym_opt)
                    token_opt, ls, sp = (nfo_data["token"], nfo_data["lotsize"], nfo_data["strikeprice"])

                    ft_data.save_day(t, exchange="NFO", symbol=symbol, tsym=tsym_opt)
                    avgprc = ft_data.get_quote(t, "NFO", symbol, tsym_opt)
                    if avgprc:
                        norenordernum = random.randrange(1_000_000, 10_000_000)
                        ord = Order( norenordernum, ordtime=t, sym=symbol, tsym=tsym_opt,
                            token=token_opt,
                            instrument=Instrument.OptionsStock,
                            ls=ls,
                            avgprice=avgprc,
                            qty=qty,
                            tranType=TranType.Sell,
                        )
                        if ob.add(ord):
                            logger.info("Order placed %s, %s, %s", ordtime, tsym, avgprc)
                        else:
                            logger.error("Unable to place Order  %s, %s, %s", ordtime, tsym, avgprc)

# Step 4
# Call step3 in 2 mins interval (if there remains any order to sell)
# Call step2 hourly
# Till the market closes

program_st=today.replace(hour=12, minute=15, second=0, microsecond=0)
program_et=today.replace(hour=15, minute=25, second=0, microsecond=0)

call_buy_1=today.replace(hour=12, minute=15, second=0, microsecond=0)
buy_call_time = [call_buy_1, call_buy_1+datetime.timedelta(hours=1), call_buy_1+datetime.timedelta(hours=2)]

for dt in pd.date_range(start=program_st, end=program_et, freq='1min'):
    dt = dt.to_pydatetime()
    print('-----------------------------------------')
    print(f'Time : {dt}')
    if dt in buy_call_time:
        step2_buyoptions(dt)
    else:
        step3_exit(dt)

def squareOff(ob):
    step3_exit(today.replace(hour=15, minute=26, second=0, microsecond=0), force_exit=True)

squareOff(ob)

print(ob)
ob.to_csv(os.path.join(strat_momentum_path, f'report_{today_str}.csv'))