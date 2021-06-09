from ib.api import TestApp
from resources.STATIC_DATA import *
from utils.logutils import init_log
import pandas as pd
logger = init_log(__name__)

if __name__ == '__main__':
    app = TestApp()

    # Account Level Monetary Fields - cached and refreshed every 5 mins
    accounting_values = app.get_accounting_values(IB_ACCOUNT_NAME)
    logger.info(f'Printing AccountingDataDF - \n {accounting_values}')

    # Example of getting data related to specific field -
    logger.info(f'Printing cash balances - \n '
                f'{accounting_values.loc[accounting_values["Key"]== "TotalCashBalance"]}')

    # Portfolio Data - cached and refreshed every 5 mins
    accounting_updates = app.get_accounting_updates(IB_ACCOUNT_NAME)
    logger.info(f'\n{accounting_updates}')

    #TODO: Excel Generation
    #TODO: Formulas for derived fields to update excel values later
    #TODO: Target = Current by default
    # Cash to deploy + Total Market Val = Total Capital
    # Diff in USD = Target - Current / Total Capital
    # Qty = Diff in USD / Last Price USD
    app.disconnect()
