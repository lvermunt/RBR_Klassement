"""Module to process the race results."""
import logging
import math


# pylint: disable=too-few-public-methods
class ResultProcessor:
    """
    A class to process the race results stored in DataFrames.
    """

    def __init__(self, df_all=None, df_men=None, df_women=None):
        """
        Initialises the ResultProcessor with race results DataFrames. Either a combined (df_all), or
        two gender-separated dataframes (df_men and df_women) have to be provided.

        Parameters:
        ----------
            df_all (pd.DataFrame, optional): DataFrame containing combined race results.
            df_men (pd.DataFrame, optional): DataFrame containing men's race results.
            df_women (pd.DataFrame, optional): DataFrame containing women's race results.
        """
        self.df_all = df_all
        self.df_men = df_men
        self.df_women = df_women

        if (df_all is None and df_men is None) or (df_all is None and df_women is None):
            raise ValueError("Either a combined or gender-separated dataframe required!")

    def process_results(self, race, year=2024):
        """
        Processes results for a specific race and year.

        Parameters:
        -----------
            race (string): Name of the race to be processed.
            year (int, optional): Year of the race results (default is 2024).
        """
        try:
            processing_method = getattr(self, f'process_results_{race.lower()}')
            processing_method(year)
        except AttributeError:
            logging.error('Race "%s" has not been implemented yet', race)

    def process_results_borne(self, year=2024):
        """
        Process the Borne race results DataFrame according to the specified rules.

        Parameters:
        -----------
            year (int, optional): Year of the race results (default is 2024).
        """
        self._process_header(4, 'Totaal')

        # Drop footer in excel sheet
        self.df_men.drop(self.df_men.index[-1], inplace=True)
        self.df_women.drop(self.df_women.index[-1], inplace=True)

        self._clean_dataframe()

    def process_results_sittard(self, year=2024):
        """
        Process the Sittard race results DataFrame according to the specified rules.

        Parameters:
        -----------
            year (int, optional): Year of the race results (default is 2024).
        """
        self._process_header(3)

        # Find and drop participants of specific categories
        categories_dict = {
            2023: ["JJC, JJC JEUGD JONGENS", "MJC, JJC JEUGD MEISJES"],
            2024: ["KIDSV, IRONKIDS", "KIDSM, IRONKIDS",
                   "JJC, JJC JEUGD JONGENS", "MJC, JJC JEUGD MEISJES"]
        }
        self._drop_categories(categories_dict[year])

        # Drop '(U23)' sub-string from name
        self.df_all['Naam'] = self.df_all['Naam'].str.replace(" (U23)", "")

        # Merge and remove duplicates for specified categories
        men_categories_dict = {
            2023: ["JJ, NK JUNIOREN JONGENS", "BM, NK JUNIOREN JONGENS", "MAN, NK MANNEN",
                   "BMM, NK MANNEN", "MT23, NK NEOSENIOREN"],
            2024: ["JJ, NK JUNIOREN JONGENS", "MAN, NK MANNEN"]
        }
        women_categories_dict = {
            2023: ["VRW, NK VROUWEN", "BMV, NK VROUWEN", "MJ, NK JUNIOREN MEISJES",
                   "VT23, NK NEOSENIOREN"],
            2024: ["VRW, NK VROUWEN", "MJ, NK JUNIOREN MEISJES"]
        }
        self.df_men = self._merge_categories(men_categories_dict[year])
        self.df_women = self._merge_categories(women_categories_dict[year])

        self._clean_dataframe()

        # Ensure the 'Tijd' column has consistent format
        for df in [self.df_men, self.df_women]:
            df['Tijd'] = df['Tijd'].apply(self._preprocess_time)

    def process_results_hulsbeek(self, year=2024):
        """
        Process the Hulsbeek race results DataFrame according to the specified rules.

        Parameters:
        -----------
            year (int, optional): Year of the race results (default is 2024).
        """
        self._process_header(2, 'tijd', 'deelnemer')

        # Merge and remove duplicates for specified categories
        men_categories_dict = {
            2024: ["Elite heren", "Recreanten mannen"]
        }
        women_categories_dict = {
            2024: ["Elite dames", "Recreanten vrouwen"]
        }
        self.df_men = self._merge_categories(men_categories_dict[year])
        self.df_women = self._merge_categories(women_categories_dict[year])

        self._clean_dataframe()

    def process_results_bathmen(self, year=2024):
        """
        Process the Bathmen race results DataFrame according to the specified rules.

        Parameters:
        -----------
            year (int, optional): Year of the race results (default is 2024).
        """
        self._process_header(0, 'NETTO TIJD', 'NAAM')

        self._clean_dataframe()

    def _update_dataframe(self, key, df):
        """
        Updates the specific DataFrame attribute based on the provided key.

        Parameters:
        -----------
            key (str): Key identifying which DataFrame to update ('all', 'men', 'women').
            df (pd.DataFrame): The DataFrame to set for the specified key.
        """
        if key == 'all':
            self.df_all = df
        elif key == 'men':
            self.df_men = df
        elif key == 'women':
            self.df_women = df

    def _process_header(self, index_row, time_column_name=None, participant_column_name=None):
        """
        Process the header of the loaded excel/txt files

        Parameters:
        -----------
            index_row (int): The index of the row that contains the column names.
            time_column_name (str, optional): Original column name for total time.
            participant_column_name (str, optional): Original column name for participants names.
        """
        # List to iterate over, using references to the actual DataFrame objects.
        dataframes = {'all': self.df_all, 'men': self.df_men, 'women': self.df_women}

        for key, df in dataframes.items():
            if df is not None:
                # Rename columns
                new_column_names = df.iloc[index_row].to_dict()
                df.rename(columns=new_column_names, inplace=True)

                # General column names
                if time_column_name:
                    df = df.rename(columns={f'{time_column_name}': 'Tijd'})
                if participant_column_name:
                    df = df.rename(columns={f'{participant_column_name}': 'Naam'})

                # Drop header
                df.drop(df.index[0], inplace=True)

                self._update_dataframe(key, df)

    def _drop_categories(self, categories):
        """
        Drop participants listed from certain categories.

        Parameters:
        -----------
            categories (list): List of categories to remove.
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
            categories (list): List of categories to merge.
        """
        # Remove rows that are not participants before merging
        self.df_all = self.df_all[~self.df_all.iloc[:, 0].isin([self.df_all.columns[0]])]
        self.df_all = self.df_all.reset_index(drop=True)

        # Merge specified categories
        merged_rows = []
        for category in categories:
            start_index = self.df_all.index[self.df_all.iloc[:, 0] == category].tolist()[0]
            end_index = self.df_all.index[
                (self.df_all.index > start_index) & self.df_all.iloc[:, 0].isnull()
            ].min()
            if math.isnan(end_index):
                end_index = self.df_all.index[-1]
            merged_rows.append(list(range(start_index, end_index + 1)))
        merged_rows = [row for cat_rows in merged_rows for row in cat_rows]
        df_new = self.df_all.iloc[merged_rows]
        return df_new

    def _clean_dataframe(self):
        """
        Cleans data in the DataFrame(s) by
        - removing rows that are not considered as valid participant entries (empty/title/column lines)
        - removing participants that had DQ, DNS, or DNF
        - removing duplicated participants
        """
        # List to iterate over, using references to the actual DataFrame objects.
        dataframes = {'all': self.df_all, 'men': self.df_men, 'women': self.df_women}

        for key, df in dataframes.items():
            if df is not None:
                df = df.dropna(subset=df.columns[1:], how='all')
                df = df[~df.iloc[:, 0].isin([df.columns[0]])]
                df = df[~df.iloc[:, 0].isin(['DQ', 'DNS', 'DNF'])]
                df = df.drop_duplicates(subset=['Naam']).reset_index(drop=True)
                df = df.reset_index(drop=True)

                self._update_dataframe(key, df)

    @staticmethod
    def _preprocess_time(time_str):
        """
        Update time format, so pandas can properly sort it

        Parameters:
        -----------
            time_str (str): Original time string
        """
        parts = time_str.split(':')
        if len(parts) == 2:  # MM:SS format
            return '0:' + time_str
        return time_str
