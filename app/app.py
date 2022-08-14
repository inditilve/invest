import logging
from pathlib import Path

import pandas as pd

from ib.ib import InteractiveBrokersApi
from prices.fetch_returns import fetch_close_prices_yfinance, calc_returns, calc_sd_move
from resources.STATIC_DATA import REPORT_OUTPUT_PATH

logging.basicConfig(level=logging.INFO)
logging.getLogger("indika-invest").setLevel(logging.WARN)


if __name__ == '__main__':
    # TODO: Send to fetch_returns accordingly to grab prices / returns
    # TODO: Instrument classifiers (coupled with DB concern)

    # TODO: Plug-in current portfolio into inception date and calculate return until date
    # TODO: Plug-in NetLiq into S&P inception date and calculate return until date
    # TODO: Plug-in current portfolio MINUS single names and calculate return until date

    # TODO: Figure out whether to fetch adj or unadj close prices???
    #  HK Quandls prices unknown whether adjusted or not,
    #  Yahoo prices are UN-adjusted (we specifically pick close and not adj close in the code below)
    # TODO: Format Excel for better viewing?

    with InteractiveBrokersApi() as app:
        position_data = app.get_positions()
        logging.info(position_data)
        account_data = app.get_account_data()
        logging.info(account_data)

    product_static_data = pd.read_csv((Path.cwd().parent.absolute() / 'resources' / 'product_info.csv'))

    position_data = pd.merge(position_data, product_static_data, how='left', on='symbol')

    position_data.loc[position_data.exchange == 'SEHK', 'symbol'] += '.HK'
    position_data.loc[position_data.exchange == 'LSEETF', 'symbol'] += '.L'

    prices_list = fetch_close_prices_yfinance(position_data.symbol.tolist())

    prices_list.sort_index(axis=1, inplace=True)

    portfolio_returns = calc_returns(prices_list)
    portfolio_sd_move = calc_sd_move(portfolio_returns)

    with pd.ExcelWriter(REPORT_OUTPUT_PATH) as writer:
        position_data.to_excel(writer, sheet_name="Position", index=False)
        account_data.to_excel(writer, sheet_name="Account", index=False)
        portfolio_sd_move.to_excel(writer, sheet_name="Returns")
