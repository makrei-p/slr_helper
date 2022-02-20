import json
from ieee_xplore import XPLORE
from time import sleep
from slr_helper.parsers.ieee_json_parser import IeeeJsonParser


class IeeeSearch:

    def __init__(self):
        self.__int__(api_key=None)

    def __int__(self, api_key):
        if not api_key:
            import os
            env_var = 'XPLORE_API_KEY'
            secret_path = './.config/.secret.json'
            if env_var in os.environ:
                api_key = os.environ['XPLORE_API_KEY']
            elif os.path.exists(secret_path):
                with open(secret_path, 'r') as file:
                    api_key = json.load(file)['xplore-api-key']
            else:
                print(f'Xplore api key was not provided. Set it either as environment variable {env_var} or in the file'
                      f' {secret_path} in .xplore-api-key')
        self._api_key_ = api_key

    def search(self, boolean_text: str):
        xplore = XPLORE(self._api_key_)
        xplore.maximumResults(200)  # this is the maximum the API will return
        xplore.booleanText(boolean_text)

        data = xplore.callAPI()

        df = IeeeJsonParser().get_df_from_result_str(data)

        return df

    def search_all(self, texts, verbose=False):
        dfs = []
        for i, text in enumerate(texts):
            if verbose:
                print(f'Starting search {i}:\t{text}')
            dfs.append(self.search(text))
            if verbose:
                print(f'Found: {len(dfs[-1])}')
            sleep(.1)  # max 10 requests per seconds
        return dfs
