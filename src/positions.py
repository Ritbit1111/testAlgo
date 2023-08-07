import os
from attr import dataclass, field
import pandas as pd
import datetime

from src.DataFetcher import FTDataService
from src.orders import Order, TranType

'''
Positions are supposed to be uniquely identified by the tsym (Trading symbol)
qty can be negative if net is sell
totalprice can be negative too
margin will be positive only
'''
@dataclass
class Position:
    tsym:str
    exchange: str
    sym:str
    token:str
    instrument:str
    lotsize:int
    netqty:int
    avgprice:float
    margin:float
    totalprice:float = field(init=False)

    def __post_init__(self):
        self.totalprice = self.avgprice * self.netqty
    
class Positions:
    def __init__(self, data_fetcher:FTDataService) -> None:
        self.data_fetcher = data_fetcher
        self.pos_table = {}
    
    def get_pos(self, tsym):
        pos = self.pos_table.get(tsym, None)
        if pos is not None:
            if pos.qty == 0:
                return None
        return pos
    
    def pnlIfExit(self, tsym:str, t:datetime.datetime):
        pos:Position|None = self.get_pos(tsym)
        if pos:
            q = self.data_fetcher.get_quote(t, pos.exchange, pos.sym, pos.tsym)
            if q:
                return q*pos.qty - pos.totalprice
            return None

    def converter(self, tt:str):
        return -1 if tt==TranType.Sell else 1

    def create_new_pos(self, ord:Order):
        netqty = ord.qty * self.converter(ord.trantype)
        return Position(tsym=ord.tsym, exchange=ord.exchange, sym=ord.sym, instrument=ord.instrument, lotsize=ord.lotsize, netqty=netqty, avgprice=ord.avgprice, margin=ord.margin)
    
    def update_pos(self, ord:Order):
        pos:Position = self.get_pos(ord.tsym)
        if pos in None:
            return False
        
        # Find ord.margin again by adding or subtracting it.
        order_netqty = ord.qty * self.converter(ord)
        if (pos.qty < 0 and order_netqty > 0) or (pos.qty>0 and order_netqty<0):
            #Exit condition
            if abs(pos.qty) >= abs(order_netqty):
                pos.qty += order_netqty
                free_margin = pos.avgprice * abs(order_netqty)
                pos.margin -= free_margin
                ord.margin = free_margin
            else:
                # Reversal of qty
                pos.qty += order_netqty
                pos.avgprice = ord.avgprice
                pos.totalprice = pos.qty * pos.avgprice
                pos.margin = ord.margin

        elif (pos.qty < 0 and order_netqty < 0) or (pos.qty > 0 and order_netqty > 0):
            #Add condition
                pos.qty += order_netqty
                pos.avgprice = (pos.totalprice + ord.netprice)/abs(pos.qty)
                pos.margin += ord.margin

        self.pos_table.update({pos.tsym:pos})

    def add(self, ord:Order):
        pos:Position|None = self.get_pos(ord.tsym)
        if pos is None:
            pos = self.create_new_pos(ord)
            pos[pos.tsym] = pos
            return True
        newqty += ord.qty * self.converter(ord.trantype)
        self.pos_table.update({pos.tsym:pos})