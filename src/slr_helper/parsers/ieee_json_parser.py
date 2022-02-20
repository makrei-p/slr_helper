import pandas as pd
import json
import dateutil.parser as dateparser
from functools import reduce
from slr_helper.parsers.ieee_csv_parser import clean_isxn, clean_page_nr, get_numpages
from slr_helper.parsers import Parser


def full_name_to_auth(name):
    names = name.split()
    for i, n in enumerate(names[:-1]):
        names[i] = n[0] + '.'
    return reduce(lambda a, b: a + ' ' + b, names)


def get_author_str_from_dict(authors: dict):
    l_auth = authors['authors']  # get list of authors
    if not l_auth:
        return ''
    names = []
    for i in range(1, len(l_auth) + 1):
        for author in l_auth:
            if int(author['author_order']) == i:
                names.append(author['full_name'])
                break

    names = [full_name_to_auth(x) for x in names]
    return reduce(lambda a, b: a + ', ' + b, names)


def get_date(row):
    date_str = row['publication_date']
    if not date_str:
        date_str = row['conference_dates']

    if not date_str or type(date_str) != str:
        return None

    # for the sake of simplicity we take the last date from the span!
    date_str = date_str.split('-')[-1]

    try:
        date = dateparser.parse(date_str)
        return date
    except dateparser.ParserError:
        return None


class IeeeJsonParser(Parser):
    target_columns = ['title',  #
                      'authors',  #
                      'isbn',  #
                      'issn',  #
                      'publisher',  #
                      'url',  #
                      'doi',  #
                      'abstract',  #
                      'published_in',  #
                      'start_page',  #
                      'end_page',  #
                      'numpages',  #
                      'keywords',  #
                      'date',  #
                      'year',  #
                      'month',  #
                      'day',  #
                      'refcount'
                      ]

    def get_df(self, file_url: str):
        with open(file_url) as file:
            search_result = json.load(file)

        return self.get_df_from_result(search_result=search_result)

    def get_df_from_result_str(self, search_result: str):
        return self.get_df_from_result(search_result=json.loads(search_result))

    def get_df_from_result(self, search_result: dict):
        if not search_result or 'articles' not in search_result:
            return pd.DataFrame(columns=self.target_columns)

        hits = int(search_result['total_records'])
        result_length = len(search_result['articles'])
        if hits != result_length:
            print(f'WARNING: Query hat {hits} hits but API returned only {result_length} results.')

        # process authors
        articles = search_result['articles']
        for article in articles:
            article['authors'] = get_author_str_from_dict(article['authors'])

        # process index_terms
        for article in articles:
            index_terms = article['index_terms']
            new_terms = []
            for type in index_terms.keys():
                new_terms += index_terms[type]['terms']
            article['index_terms'] = reduce(lambda a, b: a + ';' + b, new_terms) if new_terms else ''

        df = pd.DataFrame.from_dict(search_result['articles'])

        required_columns = ['isbn', 'issn', 'start_page', 'end_page', 'pdf_url', 'html_url', 'publication_year',
                            'publication_title', 'index_terms']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        df.isbn = df.isbn.fillna('Unknown')
        df.issn = df.issn.fillna('Unknown')
        df['isbn'] = df['isbn'].apply(clean_isxn)
        df['issn'] = df['issn'].apply(clean_isxn)

        df['start_page'] = df['start_page'].apply(clean_page_nr)
        df['end_page'] = df['end_page'].apply(clean_page_nr)
        df['numpages'] = df.apply(get_numpages, axis=1)

        df['url'] = df.apply(lambda x: x['pdf_url'] if x['pdf_url'] else x['html_url'], axis=1)

        df['date'] = df.apply(get_date, axis=1)
        df['year'] = df.apply(lambda x: int(x['publication_year']) if x['publication_year'] else x['date'].year, axis=1)
        # Month and day are empty
        # set month from 'date' if 'date'.year matches 'year' else 0
        df['month'] = df.apply((lambda x: int(x['date'].month) if self._check_year_matching_date_(x) else 0),
                               axis=1)
        df['day'] = df.apply((lambda x: int(x['date'].day) if self._check_year_matching_date_(x) else 0), axis=1)

        df = df.rename(
            columns={'index_terms': 'keywords', 'publication_title': 'published_in', 'citing_paper_count': 'refcount'})

        return df[self.target_columns]

    def _check_year_matching_date_(self, x):
        return x['year'] == x['date'].year
