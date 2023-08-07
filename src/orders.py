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
    exchange: str
    sym:str
    tsym:str
    token:str
    instrument:str
    lotsize:int
    qty:int
    avgprice:float
    margin:float
    totalprice:float = field(init=False)
    netprice:float = field(init=False)
    trantype:str = TranType.Buy
    status:str = OrdStatus.New

    def __post_init__(self):
        self.totalprice = self.avgprice * self.qty
        self.netprice = self.avgprice * self.qty * (1 if self.trantype==TranType.Sell else -1)

class OrderBook:
    SAFE_PERC = 5
    def __init__(self, balance, equity_allotment=None, fno_allotment=None) -> None:
        self.opening_balance = balance
        self.margin = self.opening_balance
        self._equity_margin = self.margin if equity_allotment is None else equity_allotment
        self._fno_margin = self.margin if fno_allotment is None else fno_allotment

        self.net_balance = balance
        self.blocked_margins = {}

        self.ob_eq:List[Order]  = []
        self.ob_opt:List[Order] = []
    
    def __repr__(self) -> str:
        bal   = f'Current balance is:     {self.balance}'
        pnl   = f'Current PnL :           {self.pnl}'
        obeq  = f'Equity Order Book:\n .  {self.print_order_list(self.ob_eq)}'
        obfno = f'FnO Order Book:\n .     {self.print_order_list(self.ob_opt)}'
        return '\n'.join([bal, pnl, obeq, obfno])

    def print_order_list(self, ol):
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
        return self.net_balance

    @property
    def pnl(self):
        return self.net_balance - self.opening_balance
    
    def balcheck(self, ord:Order):
        margin_individual = self._fno_margin if self.isFnO(ord) else self._equity_margin
        return (ord.margin <= self.margin) and (ord.margin <= margin_individual)
    
    def prev_active_ord_time(self, exchange, sym):

        ob = self.ob_opt if exchange=="NFO" else self.ob_eq
        tsym_qty = self.active_fno(sym)

        possible = []

        for ord in ob:
            if (ord.sym == sym) and (ord.exchange==exchange) and (ord.tsym in tsym_qty):
                possible.append(ord.ordtime)

        possible.sort(reverse=True)
        if len(possible)>0:
            return possible[0]
        return None

    def isFnO(self, ord:Order):
        return ord.exchange == "NFO"

    def add(self, ord:Order):
        isExit = self.isExitOrder(ord)
        # free_margin = self.get_margin_exit_ord(ord)
        if not isExit:
            if not self.balcheck(ord):
                return False

        if self.isFnO(ord):
            return self.addFnoOrder(ord, isExit)
        else:
            return self.addEquityOrder(ord, isExit)

    def addFnoOrder(self, ord:Order, isExit:bool):
        if isExit:
            blocked_margin, qtyy = self.blocked_margins[ord.tsym]
            ord.margin = (blocked_margin / qtyy) * ord.qty
        redn = (-1 if isExit else 1) * ord.margin
        if ((self.margin - redn) >= 0) and ((self._fno_margin - redn) >= 0):
            self.margin -= redn
            self._fno_margin -= redn
            net_qty_addition = ord.qty*(-1 if ord.trantype==TranType.Sell else 1)
            if ord.tsym in self.blocked_margins:
                margin_current, qty_current = self.blocked_margins[ord.tsym]
                self.blocked_margins[ord.tsym] = (margin_current + redn, qty_current+net_qty_addition)
            else:
                self.blocked_margins[ord.tsym] = (redn, net_qty_addition)
            self.net_balance -= ord.totalprice * (1 if ord.trantype==TranType.Buy else -1)
            self.ob_opt.append(ord)
            return True
        return False

    def addEquityOrder(self, ord:Order, isExit:bool):
        redn = (-1 if isExit else 1) * ord.margin
        self.margin -= redn
        self._equity_margin -= redn
        self.net_balance -= ord.totalprice * (1 if ord.trantype==TranType.BUY else -1)
        if self.margin >=0 and self._equity_margin >=0 :
            self.ob_eq.append(ord)
            return True
        return False
    
    def active_equity_qty(self, symbol:str):
        qt_active = 0
        for ord in self.ob_eq:
            if ord.sym==symbol:
                qt_active += ord.qty * (1 if ord.trantype==TranType.Buy else -1)
        return qt_active
    
    def active_fno(self, symbol):
        tsym_netqty = {}
        for ord in self.ob_opt:
            if ord.sym==symbol:
                if ord.tsym in tsym_netqty:
                    tsym_netqty[ord.tsym] += ord.qty * (1 if ord.trantype==TranType.Buy else -1)
                else:
                    tsym_netqty[ord.tsym] = ord.qty * (1 if ord.trantype==TranType.Buy else -1)

        drop_zeros=[]
        for ts, qty in tsym_netqty.items():
            if qty==0:
                drop_zeros.append(ts)
        for key in drop_zeros:
            del tsym_netqty[key] 

        return tsym_netqty

    def isExitOrder(self, ord:Order):
        isFno = self.isFnO(ord)
        if isFno:
            tsym_qty = self.active_fno(ord.sym)
            if ord.tsym in tsym_qty:
                netqty = tsym_qty[ord.tsym]
                if netqty > 0:
                    return (ord.trantype == TranType.Sell) and (ord.qty <= abs(netqty))
                elif netqty < 0:
                    return (ord.trantype == TranType.Buy) and (ord.qty <= abs(netqty))
                return False
            return False
        else:
            net_qty_eq = self.active_equity_qty(ord.sym)
            if net_qty_eq > 0:
                return (ord.trantype == TranType.Sell) and (ord.qty <= abs(net_qty_eq))
            elif net_qty_eq < 0:
                return (ord.trantype == TranType.Buy) and (ord.qty <= abs(net_qty_eq))
        return False 