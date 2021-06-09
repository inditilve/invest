from ibapi.wrapper import EWrapper
from ibapi.client import EClient

from threading import Thread
# for TickerId type
import pandas as pd
from resources.STATIC_DATA import *
from ib.account_utils_api import *
from utils.decorators import retry
from utils.logutils import init_log

logger = init_log(__name__)


class TestWrapper(EWrapper):
    """
    The wrapper deals with the action coming back from the IB gateway or TWS instance

    We override methods in EWrapper that will get called when this action happens, like currentTime

    Extra methods are added as we need to store the results in this object
    """

    def __init__(self):
        super().__init__()
        # use a dict as could have different accountids
        self.positionsDF = pd.DataFrame([], columns=['Account', 'Symbol', 'Quantity', 'Average Cost', 'Sec Type'])
        self._my_accounts = {IB_ACCOUNT_NAME: queue.Queue()}

        # We set these up as we could get things coming along before we run an init
        self._my_positions = queue.Queue()
        self._my_errors = queue.Queue()

    def get_error(self, timeout=TIMEOUT_10S):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def is_error(self):
        is_error = not self._my_errors.empty()
        return is_error

    def error(self, id, errorCode, errorString):
        # Overriden method
        if id > -1:
            error_msg = f"Id: {id}, ErrorCode: {errorCode}, ErrorMsg: {errorString}"
            self._my_errors.put(error_msg)

    def init_positions(self):
        return self._my_positions

    def position(self, account, contract, pos, avgCost):
        # Overridden method
        self.positionsDF.loc[contract.symbol] = account, contract.symbol, pos, avgCost, contract.secType
        self.positionsDF.reset_index(drop=True, inplace=True)
        self._my_positions.put(self.positionsDF)

    def positionEnd(self):
        # overriden method
        self._my_positions.put(FINISHED)

    # get accounting data
    def init_accounts(self, accountName=IB_ACCOUNT_NAME):
        return self._my_accounts[accountName]

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        # overriden method

        # use this to seperate out different account data
        data = identifed_as(ACCOUNT_VALUE_FLAG, (key, val, currency))
        self._my_accounts[IB_ACCOUNT_NAME].put(data)

    def updatePortfolio(self, contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        # overriden method

        # use this to seperate out different account data
        data = identifed_as(ACCOUNT_UPDATE_FLAG, (contract, position, marketPrice, marketValue, averageCost,
                                                  unrealizedPNL, realizedPNL))
        self._my_accounts[IB_ACCOUNT_NAME].put(data)

    def updateAccountTime(self, timeStamp: str):
        # overriden method

        # use this to seperate out different account data
        data = identifed_as(ACCOUNT_TIME_FLAG, timeStamp)
        self._my_accounts[IB_ACCOUNT_NAME].put(data)

    def accountDownloadEnd(self, accountName: str):

        self._my_accounts[IB_ACCOUNT_NAME].put(FINISHED)


class TestClient(EClient):
    """
    The client method

    We don't override native methods, but instead call them from our own wrappers
    """

    def __init__(self, wrapper):
        # Set up with a wrapper inside
        EClient.__init__(self, wrapper)

        # We use these to store accounting data
        self._account_cache = simpleCache(max_staleness_seconds=5 * 60)
        # override function
        self._account_cache.update_data = self._update_accounting_data

    @retry(Exception, total_tries=10, backoff_factor=1)
    def get_current_positions(self):
        """
        Method to fetch Portfolio Positions from IB
        :return: DF with Position data
        """

        logger.info("Requesting Portfolio Positions from IB TWS")

        positions_queue = finishableQueue(self.wrapper.init_positions())

        self.reqPositions()
        current_positions = positions_queue.get(timeout=TIMEOUT_10S)

        current_positions = current_positions[0]
        while self.wrapper.is_error():
            logger.error(self.wrapper.get_error())

        if positions_queue.timed_out():
            logger.error("Exceeded maximum wait for wrapper to confirm finished whilst getting positions")

        if len(current_positions) != EXPECTED_PORTFOLIO_SIZE:
            raise Exception(
                f"Incorrect Portfolio Size retrieved from IB. Expected: {EXPECTED_PORTFOLIO_SIZE} entries, but found {len(current_positions)} entries")

        return current_positions

    def _update_accounting_data(self, accountName=IB_ACCOUNT_NAME):
        """
        Update the accounting data in the cache

        :param accountName: account we want to get data for
        :return: nothing
        """

        # Make a place to store the data we're going to return
        accounting_queue = finishableQueue(self.wrapper.init_accounts(accountName))

        # ask for the data
        self.reqAccountUpdates(True, accountName)

        # poll until we get a termination or die of boredom
        MAX_WAIT_SECONDS = 10
        accounting_list = accounting_queue.get(timeout=MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.wrapper.get_error())

        if accounting_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished whilst getting accounting data")

        # seperate things out, because this is one big queue of data with different things in it
        accounting_list = list_of_identified_items(accounting_list)
        seperated_accounting_data = accounting_list.seperate_into_dict()

        # update the cache with different elements
        self._account_cache.update_cache(accountName, seperated_accounting_data)

        # return nothing, information is accessed via get_... methods

    def get_accounting_time_from_server(self, accountName):
        """
        Get the accounting time from IB server

        :return: accounting time as served up by IB
        """

        # All these functions follow the same pattern: check if stale or missing, if not return cache, else update values

        return self._account_cache.get_updated_cache(accountName, ACCOUNT_TIME_FLAG)

    def get_accounting_values(self, accountName):
        """
        Get the accounting values from IB server

        :return: accounting values as served up by IB
        """
        # All these functions follow the same pattern: check if stale, if not return cache, else update values
        accounting_values_data = self._account_cache.get_updated_cache(accountName, ACCOUNT_VALUE_FLAG)
        dataLabels = []
        amounts = []
        currencies = []
        for accountingData in accounting_values_data:
            dataLabels.append(accountingData[0])
            amounts.append(accountingData[1])
            currencies.append(accountingData[2])

        accountingData = {'Key': dataLabels,
                          'Amount': amounts,
                          'Currency': currencies}
        accountingDataDF = pd.DataFrame.from_dict(accountingData)

        return accountingDataDF


    def get_accounting_updates(self, accountName):
        """
        Get the accounting updates from IB server

        :return: accounting updates as served up by IB
        """

        # All these functions follow the same pattern: check if stale, if not return cache, else update values

        accounting_updates_data = self._account_cache.get_updated_cache(accountName, ACCOUNT_UPDATE_FLAG)

        accountingUpdatesDF = pd.DataFrame([], columns=['Symbol', 'Position', 'LastPrice', 'AvgPrice', 'MarketValue', 'UnrealizedPnL', 'RealizedPnL', 'Weight'])
        for item in accounting_updates_data:
            contract, pos, lastPrice, marketVal, avgPrice, unrealizedPnL, realizedPnL = item
            if contract.currency == "HKD":
                lastPrice *= HKD_TO_USD_RATE
                avgPrice *= HKD_TO_USD_RATE
                marketVal *= HKD_TO_USD_RATE
            accountingUpdatesDF.loc[contract.symbol] = contract.symbol, pos, lastPrice, avgPrice, marketVal, unrealizedPnL, realizedPnL, None

        accountingUpdatesDF['Weight'] = (accountingUpdatesDF['MarketValue'] / accountingUpdatesDF['MarketValue'].sum()) * 100

        return accountingUpdatesDF


class TestApp(TestWrapper, TestClient):
    def __init__(self, ipaddress=STATIC_IP, portid=IB_PORT, clientid=IB_CLIENT_KEY):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)

        self.connect(ipaddress, portid, clientid)

        thread = Thread(target=self.run)
        thread.start()
