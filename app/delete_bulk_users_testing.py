#!/usr/bin/env python3
""" Test deleting users in bulk. """

# Imports - Python Standard Library
from os import path
from typing import List

# Constants
SOURCE_DATA_DIR = '../_gitignore/_deactivate_list_files'
SOURCE_DATA_FILE = 'deactivate_list_test.txt'
SOURCE_DATA_PATH = path.join(SOURCE_DATA_DIR, SOURCE_DATA_FILE)


class UserOperations:
    """ Class object to manipulate GetConnected response data. """

    def __init__(self) -> None:
        """ Class initialization method. """

        # Import the raw data
        self.user_emails = self.get_user_emails()

    def get_user_emails(self) -> List[str]:
        """ Import user email addresses from a text file.

            Args:
                None

            Returns:
                user_data (List[str]):
                    List of email addresses.
        """

        # Open the data file
        with open(
            file=SOURCE_DATA_PATH,
            mode='r',
            encoding='utf=8'
        ) as user_emails:
            # Read the list of user emails
            user_emails = user_emails.readlines()

            # Convert the data to a list
            user_emails = list(user_emails)

        return user_emails

    def get_user_ids(self) -> List[str]:
        """ Get user IDs from a list of user emails.

            Args:
                None

            Returns:
                user_ids (List[str]):
                    List of email addresses.
        """
