import logging
from functools import reduce

import pandas as pd

from ib.ib import InteractiveBrokersApi
from prices.fetch_returns import fetch_close_prices_yfinance, fetch_hk_close_prices_quandl, calc_returns, calc_sd_move
from resources.STATIC_DATA import REPORT_OUTPUT_PATH, US_TICKERS, HK_TICKERS, LN_TICKERS

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
        all_positions = app.get_positions()
        logging.info(all_positions)
        all_navs = app.get_account_data()
        logging.info(all_navs)

    prices_list = [fetch_close_prices_yfinance(US_TICKERS + LN_TICKERS), fetch_hk_close_prices_quandl(HK_TICKERS)]

    portfolio_prices = reduce(lambda left, right:
                              pd.merge(left, right, left_index=True, right_index=True,
                                       how='outer'), prices_list)
    portfolio_prices.sort_index(axis=1, inplace=True)

    portfolio_returns = calc_returns(portfolio_prices)
    portfolio_sd_move = calc_sd_move(portfolio_returns)

    with pd.ExcelWriter(REPORT_OUTPUT_PATH) as writer:
        all_positions.to_excel(writer, sheet_name="Position", index=False)
        all_navs.to_excel(writer, sheet_name="Account", index=False)
        portfolio_sd_move.to_excel(writer, sheet_name="Returns")
