from dataclasses import dataclass
import datetime
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
    ls:int
    avgprice:float
    qty:int
    tranType:str = TranType.Buy
    status:str = OrdStatus.New

    def totalprice(self):
        return self.avgprice * self.qty


class OrderBook:
    def __init__(self, balance) -> None:
        self.opening_balance = balance
        self._balance = balance
        self.ob_eq:List[Order]  = []
        self.ob_opt:List[Order] = []
    
    def __repr__(self) -> str:
        bal   = f'Current balance is:     {self.balance()}'
        pnl   = f'Current PnL :           {self.pnl()}'
        obeq  = f'Equity Order Book:\n .  {self.ob_eq}'
        obfno = f'FnO Order Book:\n .     {self.ob_eq}'
        return '\n'.join([bal, pnl, obeq, obfno])

    @property
    def balance(self):
        return self._balance

    def pnl(self):
        return self._balance - self.opening_balance
    
    def balcheck(self, ord:Order):
        if ord.tranType == TranType.Sell:
            return True
        if ord.totalprice() <= self._balance:
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
        self._balance -= ord.totalprice() * (1 if ord.tranType==TranType.Buy else -1)
        return True

    def addEquityOrder(self, order):
        self.ob_eq.append(order)
        self._balance -= ord.totalprice() * (1 if ord.tranType==TranType.Buy else -1)
        return True
    
    def active_equity_qty(self, symbol:str):
        qt_active = 0
        for ord in self.ob_eq:
            if ord.symbol==symbol:
                qt_active += ord.qty * (1 if ord.tranType==TranType.Buy else -1)
        return qt_active
    
    def active_fno(self, symbol):
        tsym_qty = {}
        for ord in self.ob_opt:
            if ord.symbol==symbol:
                if ord.tsym in tsym_qty:
                    tsym_qty[ord.tsym] += ord.qty * (1 if ord.tranType==TranType.Buy else -1)
                else:
                    tsym_qty[ord.tsym] = ord.qty * (1 if ord.tranType==TranType.Buy else -1)

        for ts, qty in tsym_qty.items():
            if qty==0:
                del tsym_qty[ts]

        return tsym_qty