import enum
from xtquant import xtconstant


@enum.unique
class OrderSide(enum.Enum):
    BUY = xtconstant.STOCK_BUY
    SELL = xtconstant.STOCK_SELL

    def __str__(self):
        return {
            OrderSide.BUY: "buy",
            OrderSide.SELL: "sell",
        }[self]


@enum.unique
class OrderPriceType(enum.Enum):
    LATEST_PRICE = xtconstant.LATEST_PRICE
    FIX_PRICE = xtconstant.FIX_PRICE
    MARKET_PEER_PRICE_FIRST = xtconstant.MARKET_PEER_PRICE_FIRST
    MARKET_MINE_PRICE_FIRST = xtconstant.MARKET_MINE_PRICE_FIRST
    MARKET_SH_CONVERT_5_CANCEL = xtconstant.MARKET_SH_CONVERT_5_CANCEL

    def __str__(self):
        return {
            OrderPriceType.LATEST_PRICE: "市价",
            OrderPriceType.FIX_PRICE: "限价",
            OrderPriceType.MARKET_PEER_PRICE_FIRST: "对手方最优价",
            OrderPriceType.MARKET_MINE_PRICE_FIRST: "本方最优价",
            OrderPriceType.MARKET_SH_CONVERT_5_CANCEL: "最优五档即时成交剩余撤销",
        }[self]


@enum.unique
class OrderStatus(enum.Enum):
    UNREPORTED = xtconstant.ORDER_UNREPORTED
    WAIT_REPORTING = xtconstant.ORDER_WAIT_REPORTING
    REPORTED = xtconstant.ORDER_REPORTED
    REPORTED_CANCEL = xtconstant.ORDER_REPORTED_CANCEL
    PARTSUCC_CANCEL = xtconstant.ORDER_PARTSUCC_CANCEL
    PART_CANCEL = xtconstant.ORDER_PART_CANCEL
    CANCELED = xtconstant.ORDER_CANCELED
    PART_SUCC = xtconstant.ORDER_PART_SUCC
    SUCCEEDED = xtconstant.ORDER_SUCCEEDED
    JUNK = xtconstant.ORDER_JUNK
    UNKNOWN = xtconstant.ORDER_UNKNOWN
