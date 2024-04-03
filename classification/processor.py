"""Module to process the race results"""
import logging
import math

#pylint: disable=too-few-public-methods
class ResultProcessor:
    """
    A class to process race results DataFrame.

    Attributes:
    ----------
    df_all : pd.DataFrame, optional
        DataFrame containing combined race results.
    df_men : pd.DataFrame, optional
        DataFrame containing men's race results.
    df_women : pd.DataFrame, optional
        DataFrame containing women's race results.
    """

    def __init__(self, df_all=None, df_men=None, df_women=None):
        """
        Initialises the ResultProcessor with the DataFrame containing race results.


        Parameters:
        -----------
        df_all : pd.DataFrame, optional
            DataFrame containing combined race results.
        df_men : pd.DataFrame, optional
            DataFrame containing men's race results.
        df_women : pd.DataFrame, optional
            DataFrame containing women's race results.
        """
        self.df_all = df_all
        self.df_men = df_men
        self.df_women = df_women

        if (df_all is None and df_men is None) or (df_all is None and df_women is None):
            raise ValueError("Either a combined or gender-separated dataframe required!")

    def process_results(self, race, year=2024):
        """
        Calls the correct processing function for each race

        Parameters:
        -----------
        race : string
            Name of the race to be processed.
        year : int, optional
            Year of the race results (default is 2024).
        """
        if 'Borne' in race:
            self._rename_columns_based_on_row(4)
            self._process_results_borne()
            self.df_men = self.df_men.sort_values(by='Totaal')
            self.df_women = self.df_women.sort_values(by='Totaal')
        elif 'Sittard' in race:
            self._rename_columns_based_on_row(3)
            self._process_results_sittard(year)
            self.df_men = self.df_men.sort_values(by='Tijd')
            self.df_women = self.df_women.sort_values(by='Tijd')
        elif race in ['Hulsbeek', 'Bathmen', 'Utrecht']:
            self._process_dummy_race(race)
        else:
            logging.error('Race "%s" has not been implemented yet', race)

    def _process_results_borne(self):
        """
        Process the Borne race results DataFrame according to the specified rules.
        """
        # Drop title row
        self.df_men.drop(self.df_men.index[0], inplace=True)
        self.df_women.drop(self.df_women.index[0], inplace=True)

        # Drop last row
        self.df_men.drop(self.df_men.index[-1], inplace=True)
        self.df_women.drop(self.df_women.index[-1], inplace=True)

        # Remove rows where all values are NaN (empty lines in Excel sheet)
        self.df_men = self.df_men.dropna(how='all')
        self.df_women = self.df_women.dropna(how='all')

        # Remove rows where all but the first values are NaN (subtitles in Excel sheet)
        self.df_men = self.df_men.dropna(subset=self.df_men.columns[1:], how='all')
        self.df_women = self.df_women.dropna(subset=self.df_women.columns[1:], how='all')

        # Remove rows that are not participants
        self.df_men = self.df_men[~self.df_men.iloc[:, 0].isin(['#Cat', '#Tot'])]
        self.df_women = self.df_women[~self.df_women.iloc[:, 0].isin(['#Cat', '#Tot'])]

        # Remove disqualified, not started, and not finished participants
        self.df_men = self.df_men[~self.df_men.iloc[:, 0].isin(['DQ', 'DNS', 'DNF'])]
        self.df_women = self.df_women[~self.df_women.iloc[:, 0].isin(['DQ', 'DNS', 'DNF'])]

    def _process_results_sittard(self, year=2024):
        """
        Process the Sittard race results DataFrame according to the specified rules.

        Parameters:
        -----------
        year : int, optional
            Year of the race results (default is 2024).
        """
        # Drop title row
        self.df_all.drop(self.df_all.index[0], inplace=True)

        # Find and drop participants of specific categories
        if year == 2023:
            categories_to_drop = ["JJC, JJC JEUGD JONGENS", "MJC, JJC JEUGD MEISJES"]
        elif year == 2024:
            categories_to_drop = ["KIDSV, IRONKIDS", "KIDSM, IRONKIDS",
                                  "JJC, JJC JEUGD JONGENS", "MJC, JJC JEUGD MEISJES"]
        else:
            raise ValueError("Unsupported year. Only 2023 and 2024 are implemented.")
        self._drop_categories(categories_to_drop)

        # Remove rows that are not participants
        self.df_all = self.df_all[~self.df_all.iloc[:, 0].isin(['Plaats'])]

        # Reset indices
        self.df_all = self.df_all.reset_index(drop=True)

        # Merge and remove duplicates for specified categories
        if year == 2023:
            men_categories = ["JJ, NK JUNIOREN JONGENS", "BM, NK JUNIOREN JONGENS", "MAN, NK MANNEN",
                              "BMM, NK MANNEN", "MT23, NK NEOSENIOREN"]
            women_categories = ["VRW, NK VROUWEN", "BMV, NK VROUWEN", "MJ, NK JUNIOREN MEISJES",
                                "VT23, NK NEOSENIOREN"]
        elif year == 2024:
            men_categories = ["JJ, NK JUNIOREN JONGENS", "MAN, NK MANNEN"]
            women_categories = ["VRW, NK VROUWEN", "MJ, NK JUNIOREN MEISJES"]
        else:
            raise ValueError("Unsupported year. Only 2023 and 2024 are implemented.")
        self.df_men = self._merge_categories(men_categories)
        self.df_women = self._merge_categories(women_categories)

        # Remove duplicates
        self.df_men = self.df_men.drop_duplicates(subset=['Naam']).reset_index(drop=True)
        self.df_women = self.df_women.drop_duplicates(subset=['Naam']).reset_index(drop=True)

        # Remove rows where all but the first values are NaN (subtitles in Excel sheet)
        self.df_men = self.df_men.dropna(subset=self.df_men.columns[1:], how='all')
        self.df_women = self.df_women.dropna(subset=self.df_women.columns[1:], how='all')

        # Reset indices
        self.df_men = self.df_men.reset_index(drop=True)
        self.df_women = self.df_women.reset_index(drop=True)

        # Ensure the 'Tijd' column has consistent format
        self.df_men['Tijd'] = self.df_men['Tijd'].apply(self._preprocess_time)
        self.df_women['Tijd'] = self.df_women['Tijd'].apply(self._preprocess_time)

    def _process_dummy_race(self, race):
        """
        Dummy function for races with no processing logic yet.
        """

        logging.warning("Processing logic for the %s race has not been implemented yet", race)

    def _rename_columns_based_on_row(self, index_row):
        """
        Rename columns based on the values of a specific row.

        Parameters:
        -----------
        index_row : int
            The index of the row that contains the column names.
        """
        if self.df_all is not None:
            new_column_names = self.df_all.iloc[index_row].to_dict()
            self.df_all.rename(columns=new_column_names, inplace=True)
        else:
            new_column_names = self.df_men.iloc[index_row].to_dict()
            self.df_men.rename(columns=new_column_names, inplace=True)
            new_column_names = self.df_women.iloc[index_row].to_dict()
            self.df_women.rename(columns=new_column_names, inplace=True)

    def _drop_categories(self, categories):
        """
        Drop participants listed from certain categories.

        Parameters:
        -----------
        categories : list
            List of categories to remove.
        """
        for category in categories:
            start_index = self.df_all.index[self.df_all.iloc[:, 0] == category].tolist()[0]
            end_index = self.df_all.index[
                (self.df_all.index > start_index) & self.df_all.iloc[:, 0].isnull()
            ].min()
            drop_indices = list(range(start_index, end_index))
            self.df_all.drop(drop_indices, inplace=True)

    def _merge_categories(self, categories):
        """
        Merge participants of specified categories.

        Parameters:
        -----------
        categories : list
            List of categories to merge.
        """
        merged_rows = []
        for category in categories:
            start_index = self.df_all.index[self.df_all.iloc[:, 0] == category].tolist()[0]
            end_index = self.df_all.index[
                (self.df_all.index > start_index) & self.df_all.iloc[:, 0].isnull()
            ].min()
            if math.isnan(end_index):
                end_index = self.df_all.index[-1]
            merged_rows.append(list(range(start_index, end_index)))
        merged_rows = [row for cat_rows in merged_rows for row in cat_rows]
        df_new = self.df_all.iloc[merged_rows]
        return df_new

    @staticmethod
    def _preprocess_time(time_str):
        """
        Update time format, so pandas can properly sort it

        Parameters:
        -----------
        time_str : str
            Original time string
        """
        parts = time_str.split(':')
        if len(parts) == 2:  # MM:SS format
            return '0:' + time_str
        return time_str
