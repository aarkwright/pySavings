import pandas as pd
import yfinance as yf
import yahooquery as yq
import os
import pyarrow.parquet as pq
import pyarrow as pa
from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
from loguru import logger
import time
import multiprocessing as mp

# GLOBALS
THREADS_MAX = int(mp.cpu_count())


# script_path = os.path.dirname(os.path.realpath(__file__))


# df = pd.DataFrame(yf.Ticker('MSFT').dividends.reset_index())
# df.to_feather(path="{}\\test.ftr".format(script_path))

### parquet vs feather
# https://wesmckinney.com/blog/python-parquet-multithreading/
# https://towardsdatascience.com/the-best-format-to-save-pandas-data-414dca023e0d


class Savings:
    def __init__(self):
        # Base information
        self._nasdaq_symbols = get_nasdaq_symbols()  # ~10k @ May 2021

        # Main list
        self.symbols = {
            "stocks": self._nasdaq_symbols[self._nasdaq_symbols.ETF == False].index.to_list(),
            "etfs": self._nasdaq_symbols[self._nasdaq_symbols.ETF == True].index.to_list()
        }
        self.data = self._load_symbol_data()

        # Private class info
        self._script_path = os.path.dirname(os.path.realpath(__file__))

    def _download_symbol_data(self, symbol):
        """Download symbol data and store in feather files"""

        index = "{}/{}".format(
            self.symbols['stocks'].index(symbol) + 1,
            len(self.symbols['stocks'])
        )

        save_path = '{}\\..\\data\\parquet\\{}.history.parquet'.format(self._script_path, symbol)

        def _download(_index, _symbol, _save_path):
            logger.info("[{}] Downloading historical data for symbol {} in {}".format(index, symbol, save_path))
            history = yf.Ticker(symbol).history(period='max')

            if not history.size:
                logger.warning("[{}] NO historical data for symbol {} could be found".format(index, symbol))
            else:
                logger.info("[{}] Saving historical data for symbol {} in {}".format(index, symbol, save_path))

                pq.write_table(
                    pa.Table.from_pandas(history),
                    save_path
                )

        if os.path.exists(save_path):
            if ((time.time() - os.path.getmtime(save_path)) / 60 / 60) < 24:
                logger.debug("[{}] {} exists and data is recent (<24h)".format(index, save_path))
            else:
                logger.debug("[{}] {} exists but data is outdated (>24h); fetching data...".format(index, save_path))

                _download(index, symbol, save_path)

        else:
            _download(index, symbol, save_path)

    def _download_symbol_data_mp(self):
        pool = mp.Pool(processes=int(THREADS_MAX - 1))
        pool.map(self._download_symbol_data, self.symbols['stocks'])
        pool.close()


    def _load_symbol_data(self):
        """Load symbol data from feather files"""
        return False


if __name__ == "__main__":
    s = Savings()
    data = s._download_symbol_data_mp()
