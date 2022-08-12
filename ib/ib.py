import logging
import time
from dataclasses import dataclass
from threading import Thread

import pandas as pd
from ibapi.client import EClient
from ibapi.common import TickerId, TickAttrib
from ibapi.contract import Contract
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.wrapper import EWrapper

from resources.STATIC_DATA import STATIC_IP, IB_PORT

logging.basicConfig(level=logging.INFO)
logging.getLogger("ibapi").setLevel(logging.WARN)


# class CanUpdate:
#     def update(self, new):
#         for key, value in new.items():
#             if hasattr(self, key):
#                 setattr(self, key, value)
#
#
# @dataclass
# class PositionRow(CanUpdate):
#     """
#         Class for position attributes
#     """
#     symbol: str
#     position: float
#     avg_cost: float
#     sec_type: str
#     contract: Contract
#     bid: float = 0.0
#     ask: float = 0.0
#
#     def market_price(self):
#         return (self.bid + self.ask) / 2
#
#     def market_value(self):
#         return self.position * self.market_price()


class InteractiveBrokersApi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

        self.all_positions = pd.DataFrame([])
        # TODO: MV, Last, PnL=Unrealized+Realized+Fees+Income+Interest,
        # TODO: Country,Exchange,Desc,Ticker/BBG/RIC/Ident, Industry, Type, Region?

        """
            Doc for PnL columns: 
                https://www.interactivebrokers.com/php/whiteLabel/TWS_Reference_Information/pnl_.htm
            Doc for detailed price ticks: 
                https://interactivebrokers.github.io/tws-api/tick_types.html
        """
        self.all_accounts = pd.DataFrame()

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        if reqId > -1:
            logging.error("Error. Id: ", reqId, " Code: ", errorCode, " Msg: ", errorString)

    """
        START - Callback handlers
    """

    def position(self, account: str, contract: Contract, position: float,
                 avgCost: float):
        super().position(account, contract, position, avgCost)
        data = {
            "symbol": [contract.symbol],
            "position": [position],
            "avg_cost": [avgCost],
            "sec_type": [contract.secType],
            "contract": [contract]
        }
        position_df = pd.DataFrame(data)
        self.all_positions = pd.concat([self.all_positions, position_df], ignore_index=True)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                       currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        data = {
            "reqId": [reqId],
            "account": [account],
            "tag": [tag],
            "value": [value],
            "currency": [currency]
        }
        self.all_accounts = pd.DataFrame(data)

    def pnl(self, reqId: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float):
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
        pass

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
        logging.info(f"Reached callback: reqId: {reqId}, tickType: {tickType}, price: {price}, attrib: {attrib}")
        if tickType == TickTypeEnum.DELAYED_BID:
            self.all_positions.loc[[reqId], ["Bid"]] = price
            logging.info("----------------------------GOT BID - Printing all positions")
            logging.info(self.all_positions)
        if tickType == TickTypeEnum.DELAYED_ASK:
            logging.info("-----------------------------GOT ASK - Printing all positions")
            self.all_positions.loc[[reqId], ["Ask"]] = price

        # super().cancelMktData(reqId)

    """
        END - Callback handlers
    """

    """
        START - Synchronous API wrappers
    """

    def get_positions(self):
        # associated callback: position
        self.reqPositions()
        logging.info("Waiting for IB's API response for reqPositions requests...")
        time.sleep(3)

        def req_market_data_with_delay(index, contract, delay=3):
            # associated callbacks: tickPrice, tickSize
            self.reqMarketDataType(4)
            self.reqMktData(index, contract, "", False, False, [])
            logging.info(f"Waiting for IB's API response for {contract.symbol} reqMktData requests ...")
            time.sleep(delay)

        [req_market_data_with_delay(i, contract) for i, contract in
         enumerate(self.all_positions['contract'].tolist())]

        return self.all_positions

    def get_account_data(self):
        self.reqAccountSummary(0, "All",
                               "NetLiquidation")  # associated callback: accountSummary / Can use "All" up to 50
        # accounts; after that might need to use specific group name(s) created on TWS workstation
        logging.info("Waiting for IB's API response for reqAccountSummary requests...")
        time.sleep(3)
        return self.all_accounts

    def get_pnl_columns(self):
        # associated callback: pnl
        pass

    def enrich(self):
        pass

    """
        END - Synchronous API wrappers
    """


def setup():
    app = InteractiveBrokersApi()
    app.connect(STATIC_IP, IB_PORT, 0)
    # Start the socket in a thread
    api_thread = Thread(target=app.run, daemon=True)
    api_thread.start()
    time.sleep(1)  # Sleep interval to allow time for connection to server
    return app


if __name__ == '__main__':
    # Objective: Monthly investment amount to be allocated into signals based on portfolio goal
    logging.info("Testing IB's API as an imported library:")

    app = setup()

    # TODO: Positions - get PnL columns
    # TODO: Positions - get listing country / exchange info
    # TODO: Send to fetch_returns accordingly to grab prices / returns

    # TODO: Instrument classifiers (coupled with DB concern)

    # TODO: Plug-in current portfolio into inception date and calculate return until date
    # TODO: Plug-in NetLiq into S&P inception date and calculate return until date
    # TODO: Plug-in current portfolio MINUS single names and calculate return until date

    all_positions = app.get_positions()
    logging.info(all_positions)
    all_navs = app.get_account_data()
    logging.info(all_navs)
    app.disconnect()
