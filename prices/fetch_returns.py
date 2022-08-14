import numpy as np
import pandas as pd
import quandl
import yfinance as yf

from resources.STATIC_DATA import QUANDL_API_KEY


def fetch_close_prices_yfinance(tickers: list):
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


def fetch_hk_close_prices_quandl(tickers: list):
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


def calc_returns(df: pd.DataFrame):
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


def calc_sd_move(returns: pd.DataFrame):

    sd_move = returns.copy()
    
    for ticker in returns.columns:
        mean = returns[ticker].mean()
        sd = returns[ticker].std()

        sd_move.loc[returns[ticker] <= (mean - sd), ticker+'_move'] = '1 SD DROP'
        sd_move.loc[returns[ticker] <= (mean - (2 * sd)), ticker+'_move'] = '2 SD DROP'
        sd_move.loc[returns[ticker] >= (mean + sd), ticker+'_move'] = '1 SD JUMP'
        sd_move.loc[returns[ticker] >= (mean + (2 * sd)), ticker+'_move'] = '2 SD JUMP'
    return sd_move
