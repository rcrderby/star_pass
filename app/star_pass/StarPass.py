#!/usr/bin/env python3
""" Star Pass Classes and Methods """

# Imports - Python Standard Library
from csv import DictReader
from json import dumps
from os import getenv

# Imports - Third-Party
from dotenv import load_dotenv
from requests import request

# Load environment variables
load_dotenv()

# Constants
GC_TOKEN = getenv(key='GC_TOKEN')
DEFAULT_HEADERS = {
    'Authorization': f'Bearer {GC_TOKEN}',
    'Accept': 'application/json'
}
BASE_URL = getenv(key='BASE_URL')


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
            headers=DEFAULT_HEADERS,
            timeout=3
        )

        # Create response data object
        response = response_data.json()['data']

        return response

    def load_shifts_file_data(self) -> None:
        """ Load shifts data from a file.

            Args:
                None.

            Returns:
                None.
        """
        pass

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
