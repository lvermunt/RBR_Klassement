"""Module to read in the race results"""
import pandas as pd


# pylint: disable=too-few-public-methods
class ResultReader:
    """
    A class to read race results from different file formats.
    """

    def __init__(self, file_format):
        """
        Initialises the ResultReader with the specified file format.

        Parameters:
        -----------
            file_format (str): The format of the result file ('excel', 'text', etc.).
        """
        self.file_format = file_format

    def read_results(self, file_path):
        """
        Reads race results from the given file.

        Parameters:
        -----------
            file_path (str): The path to the result file.

        Returns:
        --------
            pd.DataFrame: A DataFrame containing the race results.
        """
        if self.file_format == 'excel':
            return self._read_excel_results(file_path)
        if self.file_format == 'text':
            return self._read_text_results(file_path)
        raise ValueError("Unsupported file format. Only 'excel' and 'text' are supported.")

    def _read_excel_results(self, file_path):
        """
        Reads race results from an Excel file.

        Parameters:
        -----------
            file_path (str): The path to the Excel file.

        Returns:
        --------
            pd.DataFrame: A DataFrame containing the race results.
        """
        return pd.read_excel(file_path)

    def _read_text_results(self, file_path):
        """
        Reads race results from a text file.

        Parameters:
        -----------
            file_path (str): The path to the text file.

        Returns:
        --------
            pd.DataFrame: A DataFrame containing the race results.
        """
        return pd.read_csv(file_path, delimiter='\t')
