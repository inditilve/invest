import logging
import time
from threading import Thread

import pandas as pd
from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.contract import Contract, ContractDetails
from ibapi.wrapper import EWrapper

from resources.STATIC_DATA import STATIC_IP, IB_PORT, IB_ACCOUNT_NAME, IB_DATA_OUTPUT_PATH

logging.basicConfig(level=logging.INFO)
logging.getLogger("ibapi").setLevel(logging.WARN)


class InteractiveBrokersApi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

        self.all_positions = pd.DataFrame([])

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
            "currency": [contract.currency],
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

    def pnlSingle(self, reqId: int, pos: int, dailyPnL: float, unrealizedPnL: float, realizedPnL: float, value: float):
        super().pnlSingle(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value)
        self.all_positions.loc[self.all_positions['index'] == reqId,
                               ["daily_pnl",
                                "unrealized_pnl",
                                "realized_pnl",
                                "market_value"]] = \
            [dailyPnL, unrealizedPnL, realizedPnL, value]

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.all_positions.loc[self.all_positions['index'] == reqId,
                               ["long_name",
                                "industry",
                                "category",
                                "sub_category",
                                "stock_type"]] = \
            [contractDetails.longName,
             contractDetails.industry,
             contractDetails.category,
             contractDetails.subcategory,
             contractDetails.stockType]

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
        self.all_positions.reset_index(level=0, inplace=True)

        [self.enrich(i, contract) for i, contract in
         enumerate(self.all_positions['contract'].tolist())]

        return self.all_positions

    def get_account_data(self):
        self.reqAccountSummary(0, "All",
                               "NetLiquidation")  # associated callback: accountSummary / Can use "All" up to 50
        # accounts; after that might need to use specific group name(s) created on TWS workstation
        logging.info("Waiting for IB's API response for reqAccountSummary requests...")
        time.sleep(3)
        return self.all_accounts

    def enrich(self, index: int, contract: Contract):
        def req_pnl_for_position(_index, _contract: Contract, _delay=1):
            # associated callbacks: pnlSingle
            self.reqPnLSingle(_index, IB_ACCOUNT_NAME, "", _contract.conId)
            logging.info(f"Waiting for IB's API response for {_contract.symbol} reqPnLSingle requests ...")
            time.sleep(_delay)

        def req_contract_details(_index: int, _contract: Contract, _delay: int = 2):
            # associated callbacks: contractDetails, contractDetailsEnd
            self.reqContractDetails(_index, _contract)
            logging.info(f"Waiting for IB's API response for {_contract.symbol} reqContractDetails requests ...")
            time.sleep(_delay)

        req_pnl_for_position(index, contract)
        req_contract_details(index, contract)

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
    app = setup()

    # TODO: Send to fetch_returns accordingly to grab prices / returns
    # TODO: Instrument classifiers (coupled with DB concern)

    # TODO: Plug-in current portfolio into inception date and calculate return until date
    # TODO: Plug-in NetLiq into S&P inception date and calculate return until date
    # TODO: Plug-in current portfolio MINUS single names and calculate return until date

    all_positions = app.get_positions()
    logging.info(all_positions)
    all_navs = app.get_account_data()
    logging.info(all_navs)

    with pd.ExcelWriter(IB_DATA_OUTPUT_PATH) as writer:
        all_positions.to_excel(writer, sheet_name="Position", index=False)
        all_navs.to_excel(writer, sheet_name="Account", index=False)
    app.disconnect()
