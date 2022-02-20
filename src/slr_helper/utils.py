import time
import pandas as pd
from urllib import request

def row_equals(a, b):
    """
    Default function used to Determine whether two rows a and b (of the same df) are equal.
    """
    return a['title'].lower() == b['title'].lower()


def get_refcount_from_doi(x):
    try:
        r = request.get(f'http://api.crossref.org/works/{x}')
        return r.json()['message']['is-referenced-by-count']
    except:
        return -1


def retry_if_failed(func, param, tries, wait, fail_value):
    for i in range(tries):
        val = func(param)
        if val != fail_value:
            return val
        time.sleep(wait)
    return fail_value


class Util:

    @staticmethod
    def concatenate_frames(frames: list):
        """
        Concatenates all frames in the list to a single DataFrame.
        Note: Duplicates will remain.
        Note: Index will be changed.
        """
        superframe = pd.concat(frames)
        superframe = superframe.reset_index().drop(columns=['index'])
        return superframe

    @staticmethod
    def find_duplicate_indices(frame: pd.DataFrame, equal_func=row_equals) -> list:
        """
        Searches for all duplicate rows in the frame. For comparison of two rows the function given in
        equal_func(row->bool) is used.
        :return:  a list of indices which are duplicates (not including the fist/original occurrence of each duplicate)
        """
        duplicates = []
        length = len(frame)
        for i in range(length - 1):
            for j in range(i + 1, length):
                if equal_func(frame.iloc[i], frame.iloc[j]):
                    duplicates.append(j)
        return duplicates

    @staticmethod
    def find_duplicate_indices_two_frames(frame1: pd.DataFrame, frame2: pd.DataFrame, equal_func=row_equals,
                                          short_circuit=False) -> list:
        """
        Searches for duplicate entries of each row of frame1 in frame2.

        :param frame1: frame, whose rows shall be found in frame2
        :param frame2: frame, in which frame1's rows are being searched.
        :param equal_func: The function (row -> bool) used to determine if two rows are equal. Default: Util.row_equals
        :param short_circuit: For each row in frame1, stop searching in frame2 after the first occurrence. Therefore, if
                              frame2 has contains a certain entry multiple times, it will be reported at most once.
        :return: a list of 2-tuple (int, int) of duplicates with the indexes of (frame1, frame2)
        """
        duplicates = []
        for i in range(len(frame1)):
            for j in range(len(frame2)):
                if equal_func(frame1.iloc[i], frame2.iloc[j]):
                    duplicates.append((i, j))
                    if short_circuit:
                        break
        return duplicates

    @staticmethod
    def drop_duplicates(frame: pd.DataFrame, equal_func=row_equals):
        """
        Find and remove duplicated. Does not modify :param frame:, but returns a copy without duplicates.

        :param frame: Dataframe to copy and remove duplicate rows from
        :param equal_func: The function (row -> bool) used to determine if two rows are equal. Default: Util.row_equals
        :return: Copy of frame without duplicates.
        """
        duplicates = Util.find_duplicate_indices(frame, equal_func=equal_func)
        return frame.drop(duplicates)

    @staticmethod
    def merge_frames(frames: list, equal_func=row_equals) -> pd.DataFrame:
        """
        Creates a new, duplicate free DataFrame from the list of frames provided.

        :param frames: list of pd.Dataframes with the same columns
        :param equal_func: The function (row -> bool) used to determine if two rows are equal. Default: Util.row_equals
        """
        frame = Util.concatenate_frames(frames)
        return Util.drop_duplicates(frame, equal_func=equal_func)
