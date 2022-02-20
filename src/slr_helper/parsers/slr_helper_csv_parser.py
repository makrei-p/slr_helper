import pandas as pd
from .parser import Parser


class SlrHelperCsvParser(Parser):

    def get_df(self, file_url: str):
        return pd.read_csv(file_url)
