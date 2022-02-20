from abc import ABC, abstractmethod


class Parser(ABC):

    @abstractmethod
    def get_df(self, file_url: str):
        pass
