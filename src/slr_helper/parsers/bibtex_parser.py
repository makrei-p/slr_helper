# imports
import pandas as pd
import numpy as np
import datetime
import dateutil.parser as dateparser

from pybtex.database.input import bibtex
from functools import reduce

from slr_helper.parsers import Parser
from slr_helper.utils import get_refcount_from_doi, retry_if_failed

ERROR_ON_MISSING_FIELD = False


# search for all possible columns that can be retrieved from the current bibtex file
def get_fields(bibdata):
    columns = []
    for bib_id in bibdata.entries:
        fields = bibdata.entries[bib_id].fields
        for field in fields.keys():
            if field not in columns:
                columns.append(field)
    return columns


def person_to_str(x):
    name = ''
    for part in x.first_names:
        name += part[0] + ". "
    for part in x.middle_names:
        name += part[0] + ". "
    for part in x.last_names:
        name += part + " "
    return name[:-1] if name != "" else ""


def create_author_str(persons):
    authors = persons.get('author')
    if authors is None:
        return ''
    return reduce((lambda x, y: x + "; " + y), map(person_to_str, authors))


def create_dict_from_bibentry(bibentry, fields):
    val = {'authors': create_author_str(bibentry.persons)}
    bib_keys = bibentry.fields.keys()
    for key in fields:
        val[key] = bibentry.fields[key] if key in bib_keys else ""

    return val


def calculate_numpages(x):
    return x['end_page'] - x['start_page'] + 1 if x['end_page'] != -1 and x['start_page'] != -1 else -1


def bibtex_time_to_datetime(x):
    if x and (type(x) != float or not np.isnan(x)):
        try:
            return dateparser.parse(x)
        except dateparser.ParserError:
            pass
    return None


# verify month and year
def check_year(x):
    year = x['year']
    if year:
        year = int(year)
    else:
        year = x['date'].year if x['date'] else 0
    return year


def check_month(x):
    month = x['month']
    if month:
        month = datetime.datetime.strptime(month, '%B').date().month
    else:
        month = x['date'].month if x['date'] and x['date'].year == x['year'] else 0
    return month


# However we should remove useless chars! like '-' which may but need not be written!
def clean_isxn(x):
    if str != type(x):
        return x
    if x == "":
        return "Unknown"
    x = x.replace("-", "")
    # anything else?
    return x


class BibTexParser(Parser):

    def __init__(self, get_refcount=True, refcount_tries=3, refcount_wait_after_fail=5) -> None:
        super().__init__()
        self.get_refcount = get_refcount
        self.refcount_tries = refcount_tries
        self.refcount_wait_after_fail = refcount_wait_after_fail
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
        self.bibtex_rename_map = {
            'issue_date': 'date',
            'title': 'title',
            'authors': 'authors',
            'isbn': 'isbn',
            'issn': 'issn',
            'publisher': 'publisher',
            'url': 'url',
            'doi': 'doi',
            'abstract': 'abstract',
            'keywords': 'keywords',
            'numpages': 'numpages',
            'year': 'year',
            'month': 'month'
        }

        self.bibtex_manual_keys = [x for x in self.target_columns if x not in self.bibtex_rename_map.values()]

    def get_df(self, file_url):
        # open a bibtex file
        parser = bibtex.Parser()
        bibdata = parser.parse_file(file_url)

        fields = get_fields(bibdata)
        for key in self.bibtex_rename_map.keys():
            if key not in fields and key != 'authors':
                if ERROR_ON_MISSING_FIELD:
                    assert False, "Missing value in fields! {}".format(key)
                else:
                    print("WARNING: Missing value in fields! {}".format(key))
                    fields.append(key)

        bib_df = pd.DataFrame.from_dict(
            [create_dict_from_bibentry(bibdata.entries[bib_id], fields) for bib_id in bibdata.entries])

        # apply renaming
        bib_df = bib_df.rename(self.bibtex_rename_map)

        # Take care of missing columns
        # published_in
        def get_published_in(x):
            if 'journal' in x and x['journal'] != '':
                return x['journal']
            if 'booktitle' in x and x['booktitle'] != '':
                return x['booktitle']

            print('unable to figure out published_in')
            return ''

        bib_df['published_in'] = bib_df.apply(get_published_in, axis=1)

        def _get_page(x, pos=0):
            val = str(x).split('–')[pos]
            if val == "": return -1
            try:
                return int(val)
            except ValueError:
                print(f'unknown page {val}')
                return -2

        # set start_page & end_page
        # bib_df.pages is either "start-end" or "page"
        bib_df['start_page'] = bib_df.pages.apply(lambda x: _get_page(x, 0))
        bib_df['end_page'] = bib_df.pages.apply(lambda x: _get_page(x, -1))

        # make sure numpages fits!
        bib_df['numpages'] = bib_df.apply((lambda x: int(x['numpages']) if x['numpages'] else calculate_numpages(x)),
                                          axis=1)

        bib_df['date'] = bib_df.issue_date.apply(bibtex_time_to_datetime)

        bib_df['day'] = 0

        bib_df['year'] = bib_df.apply(check_year, axis=1)

        bib_df['month'] = bib_df.apply(check_month, axis=1)

        # clean ISBN and ISSN
        # Both can end with 'X' so we cannot cast them to numbers :-(
        bib_df.isbn = bib_df.isbn.fillna("Unknown")
        bib_df.issn = bib_df.issn.fillna("Unknown")

        bib_df.isbn = bib_df.isbn.apply(clean_isxn)
        bib_df.issn = bib_df.issn.apply(clean_isxn)

        if self.get_refcount:
            bib_df['refcount'] = bib_df['doi'].apply(lambda x: retry_if_failed(get_refcount_from_doi, x,
                                                                               self.refcount_tries,
                                                                               self.refcount_wait_after_fail, -1))
        else:
            bib_df['refcount'] = -1
        return bib_df[self.target_columns]
