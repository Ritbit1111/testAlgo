from abc import ABC, abstractmethod
from ast import parse
import numpy as np
import datetime
from email.utils import getaddresses
import pandas as pd
import time

from pytz import HOUR
from src.api.noren import NorenApiPy
import os
import logging
from src.utils.utils import epochIndian, get_epoch_time, get_datetime


class DataService(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def get_time_price_series(
        self,
        exchange,
        symbol,
        tsym,
        start_time_epoch: float,
        end_time_epoch: float,
        interval: int,
    ):
        ...

    @abstractmethod
    def get_live_quote(self, exchange: str, tsym: str):
        ...

    @abstractmethod
    def get_prev_peak(self, exchange, symbol, tsym):
        ...

    @abstractmethod
    def get_prev_trough(self, exchange, symbol, tsym):
        ...


class Exchange:
    NSE = "NSE"
    BSE = "BSE"
    NFO = "NFO"
    MCX = "MCX"


ExchangeInstDict = {
    Exchange.NSE: ["EQ"],
    Exchange.BSE: "A",
    Exchange.NFO: ["OPTFUT"],
    Exchange.MCX: ["FUTCOM", "OPTFUT"],
}


class OptionType:
    PUT = "PE"
    CALL = "CE"


class UnknownExchangeException(Exception):
    def __init__(self, exchange=None):
        msg = "Exchange not available in exchange array"
        if exchange:
            msg = msg + f": {exchange}"
        super().__init__(msg)


class FTDataService(DataService):
    def __init__(self, logger: logging.Logger, api: NorenApiPy, path: str) -> None:
        self.logger = logger
        self.api = api
        self.path = os.path.join(path)
        self.nse_path = os.path.join(self.path, "NSE")
        self.nfo_path = os.path.join(self.path, "NFO")

    def _verify_exchange(self, exchange):
        if exchange not in Exchange.__dict__.values():
            raise UnknownExchangeException(exchange=exchange)

    def _getpath(self, exchange: str, nse_symbol: str):
        self._verify_exchange(exchange=exchange)
        return os.path.join(self.path, exchange, nse_symbol)

    def _make_dir(self, path: os.path):
        os.makedirs(path, exist_ok=True)

    def get_closest_option_scrip(
        self, symbol: str, expiry: datetime.datetime, quoteprice: float, option_type
    ):
        path = os.path.join(self.nfo_path, "symbol_token.csv")
        expiry = expiry.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        df = pd.read_csv(path, parse_dates=["expiry"])
        df = df[
            (df["symbol"] == symbol)
            & (df["optiontype"] == option_type)
            & (df["expiry"] >= expiry)
        ]
        df["deltadays"] = df["expiry"] - expiry
        df["deltaprice"] = abs(df["strikeprice"] - quoteprice)
        df = df.nsmallest(1, ["deltadays", "deltaprice"], keep="all")
        if df.shape[0] == 1:
            return df.iloc[0]
        self.logger.error("Unable to find unique scrip")
        print(df)
        return None

    def get_token(self, exchange, trading_symbol) -> str | None:
        self._verify_exchange(exchange=exchange)
        path = os.path.join(self.path, exchange, "symbol_token.csv")
        df = pd.read_csv(path, index_col="tsym")
        if trading_symbol in df.index:
            return df.loc[trading_symbol]["token"]
        return None

    def get_nfo_info(self, trading_symbol):
        df = pd.read_csv(os.path.join(self.nfo_path, 'symbol_token.csv'), index_col="tsym")
        if trading_symbol in df.index:
            return df.loc[trading_symbol]

    def search_token(self, exchange, symbol, instname=None) -> list[dict]:
        self._verify_exchange(exchange=exchange)
        path = os.path.join(self.path, exchange, "symbol_token.csv")
        try:
            df = pd.read_csv(path, index="symbol")
            if symbol in df.index:
                return df[symbol]["token"]
            raise Exception()
        except:
            obj = self.api.searchscrip(exchange=exchange, searchtext=symbol)
            possible_tokens = {}
            if instname is None:
                instname = ExchangeInstDict[exchange]
            for i in obj["values"]:
                if i["instname"] == instname:
                    possible_tokens[i["token"]] = i["tsym"]
                    return i["token"]
            if len(possible_tokens) != 1:
                self.logger.info("Found multiple tokens for symbol %s", symbol)
                self.logger.info("%s", possible_tokens)

    def get_live_quote(self, exchange, tsym):
        token = self.get_token(exchange=exchange, trading_symbol=tsym)
        res = self.api.get_quotes(exchange=exchange, token=token)
        return res

    def get_time_price_series(
        self,
        exchange: str,
        symbol: str,
        tsym: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        interval: int,
    ):
        if end_time - start_time > datetime.timedelta(days=1):
            raise Exception(
                "This function only works for start and end times of same day"
            )
        start_time = start_time.replace(tzinfo=None)
        end_time = end_time.replace(tzinfo=None)
        path = os.path.join(
            self.path, exchange, symbol, f'{tsym}_{start_time.strftime("%d-%m-%Y")}.csv'
        )
        self.save_day(start_time, exchange, symbol, tsym)
        df = self.read_time_series(path)
        df = df[(df["time"] >= start_time) & (df["time"] < end_time)]
        df = df.resample(rule=f"{interval}min", on="time", origin="start").agg(
            {
                "ssboe": lambda x: x.iloc[0],
                "into": lambda x: x.iloc[0],
                "inth": np.max,
                "intl": np.min,
                "intc": lambda x: x.iloc[-1],
                "intv": np.sum,
                "v": np.sum,
            }
        )
        df = df.reset_index()
        return df

    def get_quote(self, ordtime, exchange, symbol, tsym):
        path = os.path.join(
            self.path, exchange, symbol, f'{tsym}_{ordtime.strftime("%d-%m-%Y")}.csv'
        )
        self.save_day(ordtime, exchange, symbol, tsym)
        df = self.read_time_series(path)
        df = df[df["time"] < ordtime]
        if df.empty:
            self.logger.error("No data available for : %s at %s", tsym, ordtime)
            return None
        return df.iloc[0]["intc"]

    def get_prev_peak(
        self, date: datetime.datetime, exchange: str, symbol: str, tsym: str
    ):
        path = os.path.join(
            self.path, exchange, symbol, f'{tsym}_{date.strftime("%d-%m-%Y")}.csv'
        )
        self.save_day(date, exchange, symbol, tsym)
        df = self.read_time_series(path)
        df = df[df["time"] < date]
        if df.empty:
            self.logger.error("No data available for : %s at %s", tsym, ordtime)
            return None
        return df["inth"].max()

    def get_prev_trough(
        self, date: datetime.datetime, exchange: str, symbol: str, tsym: str
    ):
        path = os.path.join(
            self.path, exchange, symbol, f'{tsym}_{date.strftime("%d-%m-%Y")}.csv'
        )
        self.save_day(date, exchange, symbol, tsym)
        df = self.read_time_series(path)
        df = df[df["time"] < date]
        if df.empty:
            self.logger.error("No data available for : %s at %s", tsym, ordtime)
            return None
        return df["intl"].min()

    def save_day(self, date: datetime.datetime, exchange: str, symbol: str, tsym: str):
        path = os.path.join(
            self.path, exchange, symbol, f'{tsym}_{date.strftime("%d-%m-%Y")}.csv'
        )
        dayst = datetime.datetime( date.year, date.month, date.day, 9, 15)
        dayend = datetime.datetime( date.year, date.month, date.day, 15, 29)
        if os.path.exists(path):
            return
            df = self.read_time_series(path)
            if df["time"].min() <= dayst and df["time"].max() >= dayend:
                return
        self._verify_exchange(exchange=exchange)
        token = self.get_token(exchange=exchange, trading_symbol=tsym)
        res = self.api.get_time_price_series(
            exchange=exchange,
            token=token,
            starttime=epochIndian(dayst),
            endtime=epochIndian(dayend),
            interval=1,
        )
        df = pd.DataFrame(res)
        df["time"] = pd.to_datetime(df["time"], format="%d-%m-%Y %H:%M:%S")
        df = df.drop("stat", axis=1)
        numeric_list = [
            "ssboe",
            "into",
            "inth",
            "intl",
            "intc",
            "intvwap",
            "intv",
            "intoi",
            "v",
            "oi",
        ]
        df[numeric_list] = df[numeric_list].apply(pd.to_numeric)
        self._make_dir(os.path.dirname(path))
        df.to_csv(path, index=False)

    def read_time_series(self, path):
        dtypedict = {
            "time": "str",
            "ssboe": "int64",
            "into": "float64",
            "inth": "float64",
            "intl": "float64",
            "intc": "float64",
            "intvwap": "float64",
            "intv": "int64",
            "intoi": "int64",
            "v": "int64",
            "oi": "int64",
        }
        df = pd.read_csv(path, dtype=dtypedict, parse_dates=["time"])
        return df
    
    def active_FnO_symbol_list(self, expiry:datetime.datetime, instrument="OPTSTK"):
        df = pd.read_csv(os.path.join(self.nfo_path, 'symbol_token.csv'),  parse_dates=['expiry'])
        df = df[(df['expiry']>=expiry) & (df['instrument']==instrument)]
        return df['symbol'].unique()

    def update_nfo_symbol_token(self):
        path = os.path.join(self.path, "NFO", "symbol_token.csv")
        update_nfo_symbol_token(path)


def update_nfo_symbol_token(path):
    df = pd.read_csv("https://shoonya.finvasia.com/NFO_symbols.txt.zip")
    df.rename(
        columns={
            "Token": "token",
            "LotSize": "lotsize",
            "TradingSymbol": "tsym",
            "Symbol": "symbol",
            "Instrument": "instrument",
            "OptionType": "optiontype",
            "StrikePrice": "strikeprice",
            "Expiry": "expiry",
            "TickSize": "ticksize",
        },
        inplace=True,
    )
    df = df.filter(
        items=[
            "symbol",
            "tsym",
            "token",
            "expiry",
            "optiontype",
            "instrument",
            "lotsize",
            "optiontype",
            "strikeprice",
        ],
        axis=1,
    )
    df.to_csv(path, index=False)
