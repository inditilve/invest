import numpy as np
import pandas as pd
import quandl
import yfinance as yf

from resources.STATIC_DATA import QUANDL_API_KEY


def fetch_last_close_spot_yfinance(currency_list: list[str]) -> pd.DataFrame:
    fx_df = pd.DataFrame({'ticker': [f'{currency}USD=X' for currency in currency_list],
                          'currency': [currency for currency in currency_list]})
    tickers = fx_df['ticker'].tolist()
    data = yf.download(tickers=tickers, group_by='Ticker', period='1d')
    data = data.iloc[-1:]
    if 'Open' in data.columns:
        data.insert(0, "Ticker", tickers[0])
    else:
        data = data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)

    if not len(data) == len(tickers):
        raise ValueError("Unexpected data from yfinance")
    data.reset_index(inplace=True)
    data = data[['Ticker', 'Close']]
    data.rename(columns={"Ticker": "ticker"}, inplace=True)
    data = pd.merge(data, fx_df, how='left', on='ticker')
    data.rename(columns={"ticker": "fx_spot_ticker", "Close": "fx_rate"}, inplace=True)

    return data


def fetch_all_close_prices_yfinance(tickers: list[str]) -> pd.DataFrame:
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
    return data.Close


def fetch_hk_close_prices_quandl(tickers: list[str]) -> pd.DataFrame:
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
    quandl_tickers = [f"HKEX/0{ticker.replace(r'.HK', '')}" for ticker in tickers]
    data = quandl.get(quandl_tickers)
    data = data.filter(regex='Previous Close')
    data.columns = tickers
    return data


def calc_historical_log_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates natural log returns for provided DataFrame of close prices
    Parameters -
        df : DataFrame
            A DataFrame containing close prices indexed by date,
            where each column is a ticker's close prices

    Return returns : DataFrame
        A DataFrame representing the corresponding natural log returns,
        indexed by date and sorted in reverse chronological order
    """
    returns = df.copy()
    for ticker in df.columns:
        _return_label = f'{ticker}_return'
        df[_return_label] = df[ticker].pct_change()
        returns[ticker] = np.log(1 + df[_return_label])
    returns.sort_index(ascending=False, inplace=True)
    return returns


def tag_sd_moves(returns: pd.DataFrame) -> pd.DataFrame:
    sd_move = returns.copy()

    for ticker in returns.columns:
        mean = returns[ticker].mean()
        sd = returns[ticker].std()
        _event_label = f'{ticker}_move'

        sd_move.loc[returns[ticker] <= (mean - sd), _event_label] = '1 SD DROP'
        sd_move.loc[returns[ticker] <= (mean - (2 * sd)), _event_label] = '2 SD DROP'
        sd_move.loc[returns[ticker] >= (mean + sd), _event_label] = '1 SD JUMP'
        sd_move.loc[returns[ticker] >= (mean + (2 * sd)), _event_label] = '2 SD JUMP'
    return sd_move
