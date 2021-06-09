import numpy as np
import yfinance as yf
import quandl
import pandas as pd
from functools import reduce
from resources.STATIC_DATA import *


def yahoo_fetch(tickers):
    """
    Fetches close prices for provided list of tickers from Yahoo Finance.
    Parameters -
        tickers : list
            A list of tickers to request Yahoo Finance with

    Returns data : DataFrame
        A DataFrame containing close prices indexed by date,
        where each column is a ticker's close prices
    """

    data = yf.download(tickers=tickers, period='max')
    data = data.Close
    return data


def quandl_fetch(tickers):
    """
    Fetches close prices for provided list of tickers from Quandl.
    Parameters -
        tickers : list
            A list of tickers to request Quandl with.
            NOTE: Currently ONLY SUPPORTED for HK stocks

    Returns data : DataFrame
        A DataFrame containing close prices indexed by date,
        where each column is a ticker's close prices
    """
    quandl.ApiConfig.api_key = QUANDL_API_KEY
    quandl_tickers = ['HKEX/0' + ticker.replace(r'.HK', '') for ticker in tickers]
    data = quandl.get(quandl_tickers)
    data = data.filter(regex='Previous Close')
    data.columns = tickers
    return data


def calc_returns(df):
    """
    Calculates natural log returns for provided DataFrame of close prices
    Parameters -
        df : DataFrame
            A DataFrame containing close prices indexed by date,
            where each column is a ticker's close prices

    Returns returns : DataFrame
        A DataFrame representing the corresponding natural log returns,
        indexed by date and sorted in reverse chronological order
    """
    returns = df.copy()
    for ticker in df.columns:
        df[ticker + '_return'] = df[ticker].pct_change()
        returns[ticker] = np.log(1 + df[ticker + '_return'])
    returns.sort_index(ascending=False, inplace=True)
    return returns


def calc_sd_move(returns):

    sd_move = returns.copy()
    
    for ticker in returns.columns:
        mean = returns[ticker].mean()
        sd = returns[ticker].std()

        sd_move.loc[returns[ticker] <= (mean - sd), ticker+'_move'] = '1 SD DROP'
        sd_move.loc[returns[ticker] <= (mean - (2 * sd)), ticker+'_move'] = '2 SD DROP'
        sd_move.loc[returns[ticker] >= (mean + sd), ticker+'_move'] = '1 SD JUMP'
        sd_move.loc[returns[ticker] >= (mean + (2 * sd)), ticker+'_move'] = '2 SD JUMP'
    return sd_move


if __name__ == '__main__':
    # TODO: Figure out whether to fetch adj or unadj close prices???
    #  HK Quandls prices unknown whether adjusted or not,
    #  Yahoo prices are UN-adjusted (we specifically pick close and not adj close in the code below)

    prices_list = [yahoo_fetch(US_TICKERS + LN_TICKERS), quandl_fetch(HK_TICKERS)]

    portfolio_prices = reduce(lambda left, right:
                              pd.merge(left, right, left_index=True, right_index=True,
                                       how='outer'), prices_list)
    portfolio_prices.sort_index(axis=1, inplace=True)

    portfolio_returns = calc_returns(portfolio_prices)
    portfolio_sd_move = calc_sd_move(portfolio_returns)
    portfolio_sd_move.to_excel(OUTPUT_PATH)

    # TODO: Add logger
    # TODO: Format Excel for better viewing?