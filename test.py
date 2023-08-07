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

st = datetime.datetime(2023, 7, 29)
et = datetime.datetime(2023, 8, 4)

# et = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
days = pd.date_range(start=st, end=et, freq='B')

for today in days:
    today = today.to_pydatetime()
    today_str = today.strftime("%d-%m-%Y")
    api = initialize(logger)
    strat_momentum_path = os.path.join("apidata", "momentum", today_str)
    os.makedirs(strat_momentum_path, exist_ok=True)

    ft_data = FTDataService(logger=logger, api=api, path="./apidata")

    activeFnOsymbols = ft_data.active_FnO_symbol_list(today)
    #TODO: symbol_token.csv might contain all the equity symbols in the stock market
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

    CALL_COUNT=5
    PUT_COUNT=5
    df_buy  = pd.read_csv(os.path.join(strat_momentum_path, "buy.csv"), nrows=CALL_COUNT)
    df_sell = pd.read_csv(os.path.join(strat_momentum_path, "sell.csv"), nrows=PUT_COUNT)

    BALANCE = 10_00_000
    EQ_ALLOCATION = 0
    FnO_ALLOCATION = 10_00_000

    BALANCE_PER_SHARE = 50000
    MARGIN_PER_OPT = 1_00_000
    ob = OrderBook(BALANCE, EQ_ALLOCATION, FnO_ALLOCATION)
    # Buy Gainers, sell losers @ 9:30am (equity MIS)
    ordtime = datetime.datetime.strptime(today_str + " 09:31:00", "%d-%m-%Y %H:%M:%S")
    # for idx, df in enumerate([df_buy, df_sell]):
    #     if idx==0:
    #         logger.info("Call loop")
    #     exch = "NSE"
    #     for _, row in df.iterrows():
    #         symbol, tsym, token = row["symbol"], row["tsym"], row["token"]
    #         ft_data.save_day(today, exchange=exch, symbol=symbol, tsym=tsym)
    #         avgprc = ft_data.get_quote(ordtime, exch, symbol, tsym)
    #         qty = int(BALANCE_PER_SHARE / avgprc)
    #         norenordernum = random.randrange(1_000_000, 10_000_000)
    #         trantype = TranType.Buy if idx == 0 else TranType.Sell
    #         ord = Order(
    #             norenordernum,
    #             ordtime=ordtime,
    #             sym=symbol,
    #             exchange=exch,
    #             tsym=tsym,
    #             token=token,
    #             instrument=Instrument.Equity,
    #             lotsize=1,
    #             avgprice=avgprc,
    #             margin=avgprc,
    #             qty=qty,
    #             trantype=trantype
    #         )
    #         # if ob.add(ord):
    #         #     logger.info("Order placed %s, %s, %s", ordtime, tsym, avgprc)
    #         # else:
    #         #     logger.error("Unable to place Order  %s, %s, %s", ordtime, tsym, avgprc)
    # print(ob)
    # Step 2
    # Check for avg hourly gains and buy call options
    EQT_BUY_THRESHOLD = 0.3
    EQT_EXIT_THRESHOLD = 0.3
    OPT_BUY_THRESHOLD = 0.2
    OPT_SELL_THRESHOLD = 0.2
    OPT_EXIT_PREM_THRESHOLD = 10

    def step2_buyoptions(t):
        print("Starting Step 2")
        st = t - datetime.timedelta(hours=3)
        for idx, df in enumerate([df_buy, df_sell]):
            for _, row in df.iterrows():
                symbol, tsym, token = row["symbol"], row["tsym"], row["token"]
                hrly_df = ft_data.get_time_price_series('NSE', symbol, tsym, st, t, 60)
                hrly_df["change"] = hrly_df["intc"] - hrly_df["into"]
                hrly_df["perc"] = (hrly_df["change"] / hrly_df["intc"]) * 100
                mean_perc = hrly_df.perc.mean()
                call_put = 1 if idx==0 else -1
                buy_condition = (abs(mean_perc) > OPT_BUY_THRESHOLD) and ( mean_perc * call_put) > 0
                if buy_condition:
                    qp = ft_data.get_quote(t, "NSE", symbol, tsym)
                    if qp is  None:
                        continue
                    opt_type = OptionType.CALL if call_put == 1 else OptionType.PUT
                    nfo_data = ft_data.get_closest_option_scrip( symbol=symbol, expiry=t, quoteprice=qp, option_type=opt_type,)
                    tsym_opt, token_opt, ls, sp = ( nfo_data["tsym"], nfo_data["token"], nfo_data["lotsize"], nfo_data["strikeprice"],)

                    ft_data.save_day(today, exchange="NFO", symbol=symbol, tsym=tsym_opt)
                    avgprc = ft_data.get_quote(ordtime=t, exchange="NFO", symbol=symbol, tsym=tsym_opt)
                    if avgprc is None:
                        continue
                    qty = int(MARGIN_PER_OPT / avgprc)
                    qty = (qty // ls) * ls
                    norenordernum = random.randrange(1_000_000, 10_000_000)
                    ord = Order( norenordernum, ordtime=t, exchange="NFO", sym=symbol, tsym=tsym_opt,
                        token=token_opt,
                        instrument=Instrument.OptionsStock,
                        lotsize=ls,
                        avgprice=avgprc,
                        margin=(avgprc * qty),
                        qty=qty,
                        trantype=TranType.Buy,
                    )
                    if ob.add(ord):
                        logger.info("Order placed %s, %s, %s", t, tsym_opt, avgprc)
                        print(ob)
                    else:
                        logger.error("Unable to place Order  %s, %s, %s", t, tsym_opt, avgprc)


    # Step 3
    # Exit both stocks and options when current price is < 0.2% of peak today
    def exit_equity(t, force_exit=False):
        for idx, df in enumerate([df_buy, df_sell]):
            for _, row in df.iterrows():
                symbol, tsym, token = row["symbol"], row["tsym"], row["token"]
                quote = ft_data.get_quote(t, "NSE", symbol, tsym)
                if quote is None:
                    continue
                prev_peak = ft_data.get_prev_peak(t, "NSE", symbol, tsym)
                if prev_peak is None:
                    continue
                perc = ((quote - prev_peak) / prev_peak) * 100
                call_put = 1 if idx==0 else -1
                if force_exit:
                    exit_condition = True
                else:
                    exit_condition = (abs(perc) > EQT_EXIT_THRESHOLD) and (perc * call_put) < 0
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
                                ordtime=t,
                                sym=symbol,
                                exchange="NSE",
                                tsym=tsym,
                                token=token,
                                instrument=Instrument.Equity,
                                lotsize=1,
                                avgprice=avgprc,
                                margin=avgprc,
                                qty=abs(qty),
                                trantype=trantype
                            )
                            if ob.add(ord):
                                print(ob)
                                logger.info("Order exited %s, %s, %s", t, tsym, avgprc)
                            else:
                                logger.error("Unable to exit Order  %s, %s, %s", t, tsym, avgprc)
                    
    def exit_fno(t, force_exit=False):
        logger.info("Running exit FnO")
        for idx, df in enumerate([df_buy, df_sell]):
            if idx==0: 
                logger.info("Exiting call options")
            else:
                logger.info("Exiting put options")

            for _, row in df.iterrows():
                symbol, tsym, token = row["symbol"], row["tsym"], row["token"]
                quote = ft_data.get_quote(t, "NSE", symbol, tsym)
                if quote is None:
                    continue
                prev_peak = ft_data.get_prev_peak(t, "NSE", symbol, tsym)
                if prev_peak is None:
                    continue
                perc = ((quote - prev_peak) / prev_peak) * 100
                call_put = 1 if idx==0 else -1
                if force_exit:
                    exit_condition1 = True
                else:
                    exit_condition1 = (abs(perc) > OPT_SELL_THRESHOLD) and (perc * call_put) < 0
                if exit_condition1:
                    # Place order to sell call and put options
                    # Find tsym, qty from order book
                    tsym_qty = ob.active_fno(symbol)
                    for tsym_opt, qty in tsym_qty.items():
                        nfo_data = ft_data.get_nfo_info(tsym_opt)
                        token_opt, ls, sp = (nfo_data["token"], nfo_data["lotsize"], nfo_data["strikeprice"])
                        quote_opt = ft_data.get_quote(t, "NFO", symbol, tsym_opt)
                        if quote_opt is None:
                            continue
                        opt_buy_time = ob.prev_active_ord_time("NFO", symbol)
                        if call_put == 1:
                            prev_peak_opt = ft_data.get_prev_peak(t, "NFO", symbol, tsym_opt, from_time=opt_buy_time,)
                        else:
                            prev_peak_opt = ft_data.get_prev_trough(t, "NFO", symbol, tsym_opt, from_time=opt_buy_time,)
                        if prev_peak_opt is None:
                            continue
                        perc_opt = ((quote_opt - prev_peak_opt) / prev_peak_opt) * 100
                        exit_condition2 = (abs(perc_opt) > OPT_EXIT_PREM_THRESHOLD) and (perc_opt * call_put) < 0
                        if force_exit:
                            exit_condition2 = True

                        if exit_condition2:
                            ft_data.save_day(t, exchange="NFO", symbol=symbol, tsym=tsym_opt)
                            avgprc = ft_data.get_quote(t, "NFO", symbol, tsym_opt)
                            if avgprc:
                                norenordernum = random.randrange(1_000_000, 10_000_000)
                                ord = Order( norenordernum, ordtime=t, exchange="NFO", sym=symbol, tsym=tsym_opt,
                                    token=token_opt,
                                    instrument=Instrument.OptionsStock,
                                    lotsize=ls,
                                    avgprice=avgprc,
                                    qty=abs(qty),
                                    margin=0,
                                    trantype=TranType.Sell,
                                )
                                if ob.add(ord):
                                    logger.info("Order placed %s, %s, %s", t, tsym, avgprc)
                                    print(ob)
                                else:
                                    logger.error("Unable to place Order  %s, %s, %s", t, tsym, avgprc)

    # Step 4
    # Call step3 in 2 mins interval (if there remains any order to sell)
    # Call step2 hourly
    # Till the market closes

    program_st=today.replace(hour=9, minute=35, second=0, microsecond=0)
    program_et=today.replace(hour=15, minute=25, second=0, microsecond=0)

    st_enter_fno = today.replace(hour=10, minute=15, second=0, microsecond=0)
    et_enter_fno = today.replace(hour=15, minute=15, second=0, microsecond=0)
    enter_fno_time = pd.date_range(start=st_enter_fno, end=et_enter_fno, freq='60min').to_pydatetime()

    st_exit_fno = today.replace(hour=9, minute=36, second=0, microsecond=0)
    et_exit_fno = today.replace(hour=15, minute=15, second=0)
    exit_fno_time = pd.date_range(start=st_exit_fno, end=et_exit_fno, freq='1min').to_pydatetime()

    st_exit_eq = today.replace(hour=9, minute=40, second=0, microsecond=0)
    et_exit_eq = today.replace(hour=15, minute=15, second=0)
    exit_eq_time = pd.date_range(start=st_exit_eq, end=et_exit_eq, freq='5min').to_pydatetime()

    for dt in pd.date_range(start=program_st, end=program_et, freq='1min'):
        dt = dt.to_pydatetime()
        logger.info('-----------------------------------------')
        logger.info('Time : %s', dt)
        if dt in enter_fno_time:
            step2_buyoptions(dt)
        # if dt in exit_eq_time: 
        #     exit_equity(dt)
        elif dt in exit_fno_time:
            exit_fno(dt)
        # if dt==today.replace(hour=15, minute=20, second=0, microsecond=0):
        #     exit_equity(today.replace(hour=15, minute=26, second=0, microsecond=0), force_exit=True)
        if dt==today.replace(hour=15, minute=25, second=0, microsecond=0):
            exit_fno(today.replace(hour=15, minute=26, second=0, microsecond=0), force_exit=True)

    def squareOff(ob):
        exit_equity(today.replace(hour=15, minute=26, second=0, microsecond=0), force_exit=True)
        exit_fno(today.replace(hour=15, minute=26, second=0, microsecond=0), force_exit=True)

    squareOff(ob)

    print(ob)
    report_path = os.path.join(strat_momentum_path, f'report_{today_str}.csv')
    ob.to_csv(report_path)