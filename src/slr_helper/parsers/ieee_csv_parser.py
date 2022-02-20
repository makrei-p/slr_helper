import pandas as pd
import numpy as np
import datetime

from slr_helper.parsers import Parser


def ieee_time_to_datetime(x):
    return datetime.datetime.strptime(x, '%d %b %Y').date() if x and (type(x) != float or not np.isnan(x)) else None


def clean_page_nr(number):
    try:
        return int(number)
    except ValueError:
        return -1


# fill in numpages as remaining field; errorcode = -1
def get_numpages(x):
    if x['start_page'] and x['end_page']:
        if x['start_page'] != -1 and x['end_page'] != -1:
            return int(x['end_page']) - int(x['start_page']) + 1

    return -1


# However we should remove useless chars! like '-' wich may but need not be written!
def clean_isxn(x):
    if str != type(x):
        return x

    x = x.replace("-", "")
    # anything else?
    return x


class IeeeCsvParser(Parser):

    def __init__(self) -> None:
        super().__init__()

        self.target_columns = ['title',
                               'authors',
                               'isbn',
                               'issn',
                               'publisher',
                               'url',
                               'doi',
                               'abstract',
                               'published_in',
                               'start_page',
                               'end_page',
                               'numpages',
                               'keywords',
                               'date',
                               'year',
                               'month',
                               'day',
                               'refcount'
                               ]

        # columns that can be renamed directly
        self.ieee_rename_map = {
            'Document Title': 'title',
            'Authors': 'authors',
            'ISBNs': 'isbn',
            'ISSN': 'issn',
            'Publisher': 'publisher',
            'PDF Link': 'url',
            'DOI': 'doi',
            'Abstract': 'abstract',
            'Publication Title': 'published_in',
            'Start Page': 'start_page',
            'End Page': 'end_page',
            'Author Keywords': 'keywords',
            'Publication Year': 'year',
            'Article Citation Count': 'refcount'
        }

        self.ieee_manual_keys = [x for x in self.target_columns if x not in self.ieee_rename_map.values()]

    def get_df(self, file_url):
        ieee = pd.read_csv(file_url)
        ieee = self._rename_existing_(ieee)

        for time in ['Issue Date', 'Meeting Date', 'Online Date', 'Date Added To Xplore']:
            ieee[time] = ieee[time].apply(ieee_time_to_datetime)

        ieee['date'] = ieee.apply(self._ieee_date_selector_, axis=1)

        # Month and day are empty
        # set month from 'date' if 'date'.year matches 'year' else 0
        ieee['month'] = ieee.apply((lambda x: int(x['date'].month) if self._check_year_matching_date_(x) else 0),
                                   axis=1)
        ieee['day'] = ieee.apply((lambda x: int(x['date'].day) if self._check_year_matching_date_(x) else 0), axis=1)

        ieee['end_page'] = ieee['end_page'].apply(clean_page_nr)
        ieee['start_page'] = ieee['start_page'].apply(clean_page_nr)
        ieee['numpages'] = ieee.apply(get_numpages, axis=1)

        # clean ISBN and ISSN
        # Both can end with 'X' so we cannot cast them to numbers :-(
        ieee.isbn = ieee.isbn.fillna("Unknown")
        ieee.issn = ieee.issn.fillna("Unknown")

        ieee.isbn = ieee.isbn.apply(clean_isxn)
        ieee.issn = ieee.issn.apply(clean_isxn)

        return ieee[self.target_columns]

    def _rename_existing_(self, df):
        return df.rename(columns=self.ieee_rename_map)

    def _ieee_date_selector_(self, x):
        if x['Issue Date']:
            return x['Issue Date']
        if x['Meeting Date']:
            return x['Meeting Date']
        if x['Online Date']:
            return x['Online Date']
        return x['Date Added To Xplore']

    def _check_year_matching_date_(self, x):
        return x['year'] == x['date'].year
