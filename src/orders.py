from dataclasses import dataclass
import datetime

class OrdStatus:
    New="N"
    Completed="C"
    Rejected="R"

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
    ls:int
    avgprice:float
    qty:int
    tranType:str = TranType.Buy
    status:str = OrdStatus.New

    def totalprice(self):
        return self.avgprice * self.qty

class TsymOrders:
    def __init__(self, orders) -> None:
        self.qtotal=0
        self.orders = []
        self.add(orders)
    
    def add(self, orders:[Order]):
        for ord in orders:
            qadd = ord.qty if ord.tranType==TranType.Buy else -1*ord.qty
            self.qtotal += qadd
            self.orders.append(ord)

class OrderBook:
    def __init__(self, balance) -> None:
        self.balance = balance
        self.ob = {}
    
    def add(self, ord:Order):
        if ord.avgprice <= self.balance:
            ord.status=OrdStatus.Completed
            self.balance -= (ord.avgprice * ord.qty)
            if ord.tsym in self.ob:
                self.ob[ord.tsym].add([ord])
            else:
                self.ob[ord.tsym] = TsymOrders([ord])
            return True
        return False
    
    def get(self, tsym:str):
        return self.ob.get(tsym, None)
    
    def get_pnl(self):
        return 
