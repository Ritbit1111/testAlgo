from abc import ABC, abstractmethod
import numpy as np
import datetime
from email.utils import getaddresses
import pandas as pd
import time
from src.api.noren import NorenApiPy
import os
import logging
from src.utils.utils import get_epoch_time, get_datetime


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

    def get_token(self, exchange, trading_symbol) -> str | None:
        self._verify_exchange(exchange=exchange)
        path = os.path.join(self.path, exchange, "symbol_token.csv")
        df = pd.read_csv(path, index_col="tsym")
        if trading_symbol in df.index:
            return df.loc[trading_symbol]["token"]
        return None

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
        exchange,
        symbol,
        tsym,
        start_time_epoch: float,
        end_time_epoch: float,
        interval: int,
    ):
        st = get_datetime(start_time_epoch).replace(tzinfo=None)
        et = get_datetime(end_time_epoch).replace(tzinfo=None)
        if et-st>datetime.timedelta(days=1):
            raise Exception("This function only works for start and end times of same day")
        path = os.path.join(
            self.path, exchange, symbol, f'{tsym}_{st.strftime("%d-%m-%Y")}.csv'
        )
        df = self.read_time_series(path)
        df = df[(df['time'] >= st) & (df['time'] <= et)]
        df = df.resample(rule=f'{interval}min', on='time').agg({'ssboe':lambda x:x.iloc[0], 'into':lambda x:x.iloc[0], 'inth':np.max, 'intl':np.min, 'intc':lambda x:x.iloc[-1], 'intv': np.sum, 'v': np.sum})
        df = df.reset_index()
        return df

    def get_quote(self, date, exchange, symbol, tsym):
        path = os.path.join(
            self.path, exchange, symbol, f'{tsym}_{date.strftime("%d-%m-%Y")}.csv'
        )
        df = self.read_time_series(path)
        df = df[df["time"] < date]
        return df.iloc[0]["into"]

    def get_prev_peak(self, date, exchange, symbol, tsym):
        path = os.path.join(
            self.path, exchange, symbol, f'{tsym}_{date.strftime("%d-%m-%Y")}.csv'
        )
        df = self.read_time_series(path)
        df = df[df["time"] < date]
        return df["inth"].max()

    def get_prev_trough(self, date, exchange, symbol, tsym):
        path = os.path.join(
            self.path, exchange, symbol, f'{tsym}_{date.strftime("%d-%m-%Y")}.csv'
        )
        df = self.read_time_series(path)
        df = df[df["time"] < date]
        return df["intl"].min()

    def save_day(self, date: datetime.date, exchange: str, symbol: str, tsym: str):
        self._verify_exchange(exchange=exchange)
        token = self.get_token(exchange=exchange, trading_symbol=tsym)
        path = os.path.join(
            self.path, exchange, symbol, f'{tsym}_{date.strftime("%d-%m-%Y")}.csv'
        )
        st = get_epoch_time(date.strftime("%d-%m-%Y") + " " + "09:16:00")
        et = get_epoch_time(date.strftime("%d-%m-%Y") + " " + "15:30:00")
        res = self.api.get_time_price_series(
            exchange=exchange, token=token, starttime=st, endtime=et, interval=1
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
            "TickSize": "ticksize",
        },
        inplace=True,
    )
    df = df.filter(
        items=[
            "symbol",
            "tsym",
            "expiry",
            "token",
            "lotsize",
            "optiontype",
            "strikeprice",
        ],
        axis=1,
    )
    df.to_csv(path, index=False)
