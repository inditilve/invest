import logging
from pathlib import Path

import pandas as pd

from ib.ib import InteractiveBrokersApi
from prices.historical_prices import fetch_close_prices_yfinance, calc_historical_log_returns, tag_sd_moves, \
    fetch_latest_fx_yfinance
from resources.STATIC_DATA import REPORT_OUTPUT_PATH

logging.basicConfig(level=logging.INFO)
logging.getLogger("indika-invest").setLevel(logging.WARN)

if __name__ == '__main__':
    # TODO: Plug-in current portfolio into inception date and calculate return until date
    # TODO: Plug-in NetLiq into S&P inception date and calculate return until date
    # TODO: Plug-in current portfolio MINUS single names and calculate return until date

    # TODO: Figure out whether to fetch adj or unadj close prices???
    #  HK Quandls prices unknown whether adjusted or not,
    #  Yahoo prices are UN-adjusted (we specifically pick close and not adj close in the code below)
    # TODO: Format Excel for better viewing?

    """
        Fetch IB Data
    """
    with InteractiveBrokersApi() as app:
        position_data = app.get_positions()
        logging.info(position_data)
        account_data = app.get_account_data()
        logging.info(account_data)

    """
        Fetch static meta-data 
    """
    # TODO: Storage? Source?
    product_static_data = pd.read_csv((Path.cwd().parent.absolute() / 'resources' / 'product_info.csv'))

    position_data = pd.merge(position_data, product_static_data, how='left', on='symbol')

    position_data.loc[position_data.exchange == 'SEHK', 'symbol'] += '.HK'
    position_data.loc[position_data.exchange == 'LSEETF', 'symbol'] += '.L'

    """
        Fetch fx spot for usd conversions
    """
    non_usd_position_filter = position_data['currency'] != 'USD'
    non_usd_currencies = position_data.loc[non_usd_position_filter]['currency'].unique().tolist()
    fx_data = fetch_latest_fx_yfinance(non_usd_currencies)
    position_data = pd.merge(position_data, fx_data, how='left', on='currency')
    position_data['fx_spot_ticker'] = position_data['fx_spot_ticker'].fillna('USD')
    position_data['fx_rate'] = position_data['fx_rate'].fillna(1.0)

    position_data[['daily_pnl', 'unrealized_pnl', 'market_value']] = \
        position_data[['daily_pnl', 'unrealized_pnl', 'market_value']].multiply(position_data['fx_rate'], axis="index")
    """
        Fetch historical prices and returns
    """
    yfinance_ticker_list = position_data.symbol.tolist()
    prices_data = fetch_close_prices_yfinance(yfinance_ticker_list)

    prices_data.sort_index(axis=1, inplace=True)

    prices_data = calc_historical_log_returns(prices_data)
    prices_data = tag_sd_moves(prices_data)

    """
        Write to Excel
    """
    with pd.ExcelWriter(REPORT_OUTPUT_PATH) as writer:
        position_data.to_excel(writer, sheet_name="Position", index=False)
        account_data.to_excel(writer, sheet_name="Account", index=False)
        prices_data.to_excel(writer, sheet_name="HistoricalReturns")
