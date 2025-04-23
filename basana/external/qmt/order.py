from xtquant import xttrader, xtdata, xtconstant
from xtquant.xttype import StockAccount
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from .enums import *
from decimal import Decimal


class Order:
    def __init__(self, order_id=-100, acc: StockAccount = None, ticker: str = None, side: OrderSide = None,
                 quantity: int = None, price_type: OrderPriceType = None, price: Decimal = None,
                 remark: str = None):
        self.order_id = order_id
        self.acc = acc
        self.ticker = ticker
        self.side = side
        self.quantity = quantity
        self.price_type = price_type
        self.price = price
        self.remark = remark
