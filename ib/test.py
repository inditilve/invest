import logging
import time
from threading import Thread

import pandas as pd
from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.wrapper import EWrapper

from resources.STATIC_DATA import STATIC_IP, IB_PORT

logging.basicConfig(level=logging.INFO)
logging.getLogger("ibapi").setLevel(logging.WARNING)

class ib_class(EWrapper, EClient):

    def __init__(self):
        EClient.__init__(self, self)

        self.all_positions = pd.DataFrame([], columns=['Account', 'Symbol', 'Quantity', 'Average Cost', 'Sec Type'])
        self.all_accounts = pd.DataFrame([], columns=['reqId', 'Account', 'Tag', 'Value', 'Currency'])

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        if reqId > -1:
            logging.info("Error. Id: ", reqId, " Code: ", errorCode, " Msg: ", errorString)

    def position(self, account, contract, pos, avgCost):
        index = str(account) + str(contract.symbol)
        self.all_positions.loc[index] = account, contract.symbol, pos, avgCost, contract.secType

    def accountSummary(self, reqId, account, tag, value, currency):
        index = str(account)
        self.all_accounts.loc[index] = reqId, account, tag, value, currency

    def read_positions(self):
        self.reqPositions()  # associated callback: position
        logging.info("Waiting for IB's API response for accounts positions requests...\n")
        time.sleep(3)
        return self.all_positions

    def read_navs(self):
        self.reqAccountSummary(0, "All",
                              "NetLiquidation")  # associated callback: accountSummary / Can use "All" up to 50 accounts; after that might need to use specific group name(s) created on TWS workstation
        logging.info("Waiting for IB's API response for NAVs requests...\n")
        time.sleep(3)
        return self.all_accounts


def read_positions():  # read all accounts positions and return DataFrame with information
    def run_loop():
        app.run()

    app = ib_class()
    app.connect('127.0.0.1', 7496, 0)
    # Start the socket in a thread
    api_thread = Thread(target=run_loop, daemon=True)
    api_thread.start()
    time.sleep(1)  # Sleep interval to allow time for connection to server

    app.reqPositions()  # associated callback: position
    logging.info("Waiting for IB's API response for accounts positions requests...\n")
    time.sleep(3)
    current_positions = app.all_positions
    current_positions.set_index('Account', inplace=True, drop=True)  # set all_positions DataFrame index to "Account"

    app.disconnect()

    return (current_positions)


def read_navs():  # read all accounts NAVs

    def run_loop():
        app.run()

    app = ib_class()
    app.connect('127.0.0.1', 7496, 0)
    # Start the socket in a thread
    api_thread = Thread(target=run_loop, daemon=True)
    api_thread.start()
    time.sleep(1)  # Sleep interval to allow time for connection to server

    app.reqAccountSummary(0, "All",
                          "NetLiquidation")  # associated callback: accountSummary / Can use "All" up to 50 accounts; after that might need to use specific group name(s) created on TWS workstation
    logging.info("Waiting for IB's API response for NAVs requests...\n")
    time.sleep(3)
    current_nav = app.all_accounts

    app.disconnect()

    return (current_nav)


def setup():
    app = ib_class()
    app.connect(STATIC_IP, IB_PORT, 0)
    # Start the socket in a thread
    api_thread = Thread(target=app.run, daemon=True)
    api_thread.start()
    time.sleep(1)  # Sleep interval to allow time for connection to server
    return app


if __name__ == '__main__':
    logging.info("Testing IB's API as an imported library:")

    app = setup()
    all_positions = app.read_positions()
    logging.info(all_positions)
    all_navs = app.read_navs()
    logging.info(all_navs)
    app.disconnect()
