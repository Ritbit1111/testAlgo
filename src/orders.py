from dataclasses import dataclass, asdict, field 
import datetime
import pandas as pd
from typing import List

class Instrument:
    Equity="Eq"
    OptionsStock="OPTSTK"
    OptionsIndex="OPTIDX"
    FuturesStock="FUTSTK"
    FuturesIndex="FUTIDX"
class OrdStatus:
    New="N"
    Accepted="C"
    Rejected="R"
    Exited="E"

class TranType:
    Buy="B"
    Sell="S"

@dataclass
class Order:
    ord_number:str
    ordtime:datetime.datetime
    sym:str
    tsym:str
    token:str
    instrument:str
    lotsize:int
    qty:int
    avgprice:float
    totalprice:float = field(init=False)
    tranType:str = TranType.Buy
    status:str = OrdStatus.New

    def __post_init__(self):
        self.totalprice = self.avgprice * self.qty

class OrderBook:
    def __init__(self, balance, equity_allotment=None, fno_allotment=None) -> None:
        self.opening_balance = balance
        self._balance = balance
        self._equity_bal = balance if equity_allotment is None else equity_allotment
        self._fno_bal = balance if fno_allotment is None else equity_allotment
        self.ob_eq:List[Order]  = []
        self.ob_opt:List[Order] = []
    
    def __repr__(self) -> str:
        bal   = f'Current balance is:     {self.balance}'
        pnl   = f'Current PnL :           {self.pnl}'
        obeq  = f'Equity Order Book:\n .  {self.print_order_list(self.ob_eq)}'
        obfno = f'FnO Order Book:\n .     {self.print_order_list(self.ob_opt)}'
        return '\n'.join([bal, pnl, obeq, obfno])

    def print_order_list(self, ol):
        # li = [asdict(ord) for ord in ol]
        # df = pd.DataFrame(li)
        df = self.to_df(ol)
        if df.empty:
            return "Empty List"
        return df.__repr__()

    def to_df(self, ol):
        li = [asdict(ord) for ord in ol]
        df = pd.DataFrame(li)
        return df
    
    def to_df_eq(self):
        return self.to_df(self.ob_eq)

    def to_df_opt(self):
        return self.to_df(self.ob_opt)

    def to_csv(self, path):
        dfeq = self.to_df_eq()
        dfop = self.to_df_opt()
        df = pd.concat([dfeq, dfop], axis=0)
        df.to_csv(path, index=None)

    @property
    def balance(self):
        return self._balance

    @property
    def pnl(self):
        return self._balance - self.opening_balance
    
    def balcheck(self, ord:Order):
        if ord.tranType == TranType.Sell:
            return True
        if ord.totalprice <= self._balance:
            return True
        print('Insufficient balance!')
        return False
        
    def isFnO(self, ord:Order):
        return False if ord.instrument == Instrument.Equity else True

    def add(self, ord:Order):
        if not self.balcheck(ord):
            return False

        if self.isFnO(ord):
            return self.addFnoOrder(ord)
        else:
            return self.addEquityOrder(ord)

    def addFnoOrder(self, ord:Order):
        self.ob_opt.append(ord)
        self._balance -= ord.totalprice * (1 if ord.tranType==TranType.Buy else -1)
        return True

    def addEquityOrder(self, ord:Order):
        self.ob_eq.append(ord)
        self._balance -= ord.totalprice * (1 if ord.tranType==TranType.Buy else -1)
        return True
    
    def active_equity_qty(self, symbol:str):
        qt_active = 0
        for ord in self.ob_eq:
            if ord.sym==symbol:
                qt_active += ord.qty * (1 if ord.tranType==TranType.Buy else -1)
        return qt_active
    
    def active_fno(self, symbol):
        tsym_qty = {}
        for ord in self.ob_opt:
            if ord.sym==symbol:
                if ord.tsym in tsym_qty:
                    tsym_qty[ord.tsym] += ord.qty * (1 if ord.tranType==TranType.Buy else -1)
                else:
                    tsym_qty[ord.tsym] = ord.qty * (1 if ord.tranType==TranType.Buy else -1)

        drop_zeros=[]
        for ts, qty in tsym_qty.items():
            if qty==0:
                drop_zeros.append(ts)
        for key in drop_zeros:
            del tsym_qty[key] 

        return tsym_qty