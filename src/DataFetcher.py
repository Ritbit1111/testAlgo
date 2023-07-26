from abc import ABC, abstractmethod
import pandas as pd
import time
from src.api.noren import NorenApiPy
import os
import logging

class DataService(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def get_time_price_series(self, exchange, nse_symbol, start_time_epoch:float, end_time_epoch:float, interval:int):
        ...

    @abstractmethod
    def get_quote(self, exchange, nse_symbol):
        ...

    @abstractmethod
    def get_prev_peak(self, exchange, nse_symbol):
        ...

    @abstractmethod
    def get_prev_trough(self, exchange, nse_symbol):
        ...

class Exchange:
    NSE="NSE"
    BSE="BSE"
    NFO="NFO"
    MCX="MCX"

ExchangeInstDict = {
    Exchange.NSE:["EQ"],
    Exchange.BSE:"A",
    Exchange.NFO:["OPTFUT"],
    Exchange.MCX:["FUTCOM","OPTFUT"]
}

class UnknownExchangeException(Exception):
    def __init__(self, exchange=None):
        msg = 'Exchange not available in exchange array'
        if exchange:
            msg=msg+f': {exchange}'
        super().__init__(msg)

class FTDataService(DataService):
    def __init__(self, logger:logging.Logger, api:NorenApiPy, path:str) -> None:
        self.logger = logger
        self.api = api
        self.path = os.path.join(path)
        self.nse_path = os.path.join(self.path, "NSE")
        self.nfo_path = os.path.join(self.path, "NFO")
    
    def _verify_exchange(self, exchange):
        if exchange not in Exchange.__dict__.values():
            raise UnknownExchangeException(exchange=exchange)

    def _getpath(self, exchange:str, nse_symbol:str):
        self._verify_exchange(exchange=exchange)
        return os.path.join(self.path, exchange, nse_symbol)
    
    def _make_dir(path:os.path):
        os.makedirs(path, exist_ok=True)

    def get_token_single(self, exchange, trading_symbol) -> str|None:
        self._verify_exchange(exchange=exchange)
        path = os.path.join(self.path, exchange, 'symbol_token.csv')
        df = pd.read_csv(path, index_col='tsym')
        if trading_symbol in df.index:
            return df.loc[trading_symbol]['token']
        return None

    def get_token(self, exchange, symbol, instname=None):
        self._verify_exchange(exchange=exchange)
        path = os.path.join(self.path, exchange, 'symbol_token.csv')
        try:
            df = pd.read_csv(path, index='symbol')
            if symbol in df.index:
                return df[symbol]['token']
            raise Exception()
        except:
            obj = self.api.searchscrip(exchange=exchange, searchtext=symbol)
            possible_tokens = {}
            if instname is None:
                instname = ExchangeInstDict[exchange]
            for i in obj['values']:
                if (i['instname'] == instname):
                    possible_tokens[i['token']] = i['tsym']
                    return i['token']
            if len(possible_tokens) != 1:
                self.logger.info("Found multiple tokens for symbol %s", symbol)
                self.logger.info("%s", possible_tokens)

    def get_quote(self, exchange, nse_symbol, time=None):
        ltp = 0
        if time is None:
            token = self.get_token(exchange, nse_symbol)
            res =  self.api.get_quotes(exchange=exchange, token=token)
        return res

    def get_time_price_series(self, exchange, nse_symbol, start_time_epoch: float, end_time_epoch: float, interval: int):
        return super().get_time_price_series(nse_symbol, start_time_epoch, end_time_epoch, interval)
    
    def get_prev_peak(self, exchange, nse_symbol):
        return super().get_prev_peak(nse_symbol)
    
    def get_prev_trough(self, exchange, nse_symbol):
        return super().get_prev_trough(nse_symbol)
    
    def save_day(self, date, exchange, nse_symbol):
        ...
    
    def update_nfo_symbol_token(self):
        path = os.path.join(self.path, "NFO", 'symbol_token.csv')
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
            'OptionType':'optiontype',
            'StrikePrice':'strikeprice',
            'TickSize':'ticksize',
        },
        inplace=True,
    )
    df = df.filter(items=["symbol", "tsym", "expiry", "token", "lotsize", 'optiontype', 'strikeprice'], axis=1)
    df.to_csv(path, index=False)
