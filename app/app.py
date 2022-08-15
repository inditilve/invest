import logging

import pandas as pd

from ib.ib import InteractiveBrokersApi
from prices import prices
from resources.STATIC_DATA import REPORT_OUTPUT_PATH, PRODUCT_INFO_PATH

logging.basicConfig(level=logging.INFO)
logging.getLogger("invest").setLevel(logging.WARN)

if __name__ == '__main__':
    # TODO: Plug-in current portfolio into inception date and calculate return until date
    # TODO: Plug-in NetLiq into S&P inception date and calculate return until date
    # TODO: Plug-in current portfolio MINUS single names and calculate return until date

    # TODO: Yahoo prices are UN-adjusted (we specifically pick close and not adj close in the code below)
    # TODO: Format Excel for better viewing?

    """
        Fetch IB Data
    """
    with InteractiveBrokersApi() as ib_api:
        position_data = ib_api.get_positions()
        logging.info(position_data)
        account_data = ib_api.get_account_data()
        logging.info(account_data)

    """
        Fetch static meta-data
    """
    # TODO: Storage? Source?
    product_static_data = pd.read_csv(PRODUCT_INFO_PATH)
    position_data = pd.merge(position_data, product_static_data, how='left', on='symbol')

    """
        Fetch yfinance fx spot for usd conversions
    """
    fx_data = prices.fetch_last_close_spot_yfinance(
        position_data.loc[position_data['currency'] != 'USD']['currency'].unique().tolist())
    position_data = pd.merge(position_data, fx_data, how='left', on='currency')
    position_data['fx_spot_ticker'].fillna('USD', inplace=True)
    position_data['fx_rate'].fillna(1.0, inplace=True)

    position_data[['daily_pnl', 'unrealized_pnl', 'market_value']] = \
        position_data[['daily_pnl', 'unrealized_pnl', 'market_value']].multiply(position_data['fx_rate'], axis="index")

    """
        Fetch yfinance historical prices and returns
    """
    position_data.loc[position_data.exchange == 'SEHK', 'symbol'] += '.HK'
    position_data.loc[position_data.exchange == 'LSEETF', 'symbol'] += '.L'
    prices_data = prices.fetch_all_close_prices_yfinance(position_data.symbol.tolist())

    prices_data.sort_index(axis=1, inplace=True)

    """
        Enrich with stats
    """
    prices_data = prices.calc_historical_log_returns(prices_data)
    prices_data = prices.tag_sd_moves(prices_data)

    """
        Write to Excel
    """
    with pd.ExcelWriter(REPORT_OUTPUT_PATH) as writer:
        position_data.to_excel(writer, sheet_name="Position", index=False)
        account_data.to_excel(writer, sheet_name="Account", index=False)
        prices_data.to_excel(writer, sheet_name="HistoricalReturns")
