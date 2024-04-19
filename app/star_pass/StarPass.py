#!/usr/bin/env python3
""" Star Pass Classes and Methods """

# Imports - Python Standard Library
from os import getenv
from os import path

# Imports - Third-Party
import pandas as pd
from dotenv import load_dotenv
from pandas.core import frame, series
from pandas.core.groupby.generic import DataFrameGroupBy
from requests import request

# Load environment variables
load_dotenv(
    dotenv_path='./.env',
    encoding='utf-8'
)

# Constants
GC_TOKEN = getenv(key='GC_TOKEN')
BASE_HEADERS = {
    'Authorization': f'Bearer {GC_TOKEN}',
    'Accept': 'application/json'
}
BASE_FILE_PATH = path.join(
    getenv('BASE_FILE_RELATIVE_PATH'),
    getenv('BASE_FILE_NAME')
)
BASE_URL = getenv(key='BASE_URL')
GROUP_BY_COLUMN = getenv('GROUP_BY_COLUMN')
INPUT_FILE_EXTENSION = getenv('INPUT_FILE_EXTENSION')
INPUT_FILE = f'{BASE_FILE_PATH}{INPUT_FILE_EXTENSION}'
DROP_COLUMNS = getenv('DROP_COLUMNS').split(
    sep=', '
)
KEEP_COLUMNS = getenv('KEEP_COLUMNS').split(
    sep=', '
)
OUTPUT_FILE_EXTENSION = getenv('OUTPUT_FILE_EXTENSION')
OUTPUT_FILE = f'{BASE_FILE_PATH}{OUTPUT_FILE_EXTENSION}'
SHIFTS_DICT_KEY_NAME = getenv('SHIFTS_DICT_KEY_NAME')
START_COLUMN = getenv('START_COLUMN')
START_DATE_COLUMN = getenv('START_DATE_COLUMN')
START_TIME_COLUMN = getenv('START_TIME_COLUMN')


# Class definitions
class AmplifyShifts():
    """ AmplifyShifts base class object. """

    def __init__(self) -> None:
        """ AmplifyShifts initialization method.

            Args:
                None.

            Returns:
                None.
        """
        pass

    def _send_api_request(self) -> None:
        """ Create base API request.

            Args:
                None.

            Returns:
                None.
        """

        # Construct URL
        # url = f'{BASE_URL}{endpoint}'

        # Send API request
        response_data = request(
            # Type
            # url=url,
            headers=BASE_HEADERS,
            timeout=3
        )

        # Create response data object
        response = response_data.json()['data']

        return response

    def read_shift_csv_data(
            self,
            input_file: str = INPUT_FILE
        ) -> frame.DataFrame:
        """ Read shifts data from a CSV file and convert fields to
            strings for Amplify API compatibility.

            Args:
                input_file (str):
                    CSV file or path to CSV file.

            Returns:
                shift_data (frame.DataFrame):
                    Pandas data frame of raw shift data.
        """

        shift_data = pd.read_csv(
            filepath_or_buffer=input_file,
            dtype='string'
        )

        return shift_data

    def remove_duplicate_shifts(
            self,
            shift_data: frame.DataFrame
        ) -> frame.DataFrame:
        """ Remove duplicate shift entries.

            Args:
                shift_data (frame.DataFrame):
                    Pandas data frame of raw shift data.

            Returns:
                shift_data (frame.DataFrame):
                    Pandas data frame of shift data with duplicates
                    removed.
        """

        shift_data.drop_duplicates(
            inplace=True,
            keep='first'
        )

        return shift_data

    def format_shift_start(
            self,
            shift_data: frame.DataFrame
        ) -> frame.DataFrame:
        """ Merge the 'start_date' and 'start_time' columns to a
            'start_date' column.

            Args:
                shift_data (frame.DataFrame):
                    Pandas data frame of raw shift data.

            Returns:
                shift_data (frame.DataFrame):
                    Pandas data frame of shift data new 'start' column.
        """

        shift_data[START_COLUMN] = shift_data[
            [
                START_DATE_COLUMN,
                START_TIME_COLUMN
            ]
        ].agg(
            ' '.join,
            axis=1
        )

        return shift_data

    def drop_unused_columns(
            self,
            shift_data: frame.DataFrame
        ) -> frame.DataFrame:
        """ Drop unused columns from the data frame.

            Args:
                shift_data (frame.DataFrame):
                    Pandas data frame of raw shift data.

            Returns:
                shift_data (frame.DataFrame):
                    Pandas data frame of shift data without extra columns.
        """

        shift_data.drop(
            columns=DROP_COLUMNS,
            inplace=True
        )

        return shift_data

    def create_shift_json_data(self) -> None:
        """ Create shift JSON data for the HTTP body.

            Args:
                None.

            Returns:
                None.
        """
        pass

    def create_new_shifts_(self) -> None:
        """ Upload shift data to create new shifts.

            Args:
                None.

            Returns:
                None.
        """
        pass
