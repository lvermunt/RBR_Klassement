"""Module to assign points to the participants based on race results"""


# pylint: disable=too-few-public-methods
class ResultScorer:
    """
    A class to assign points to participants based on race results.
    """

    def __init__(self, race_results, race_name):
        """
        Initialises the ResultScorer with race results DataFrame.

        Parameters:
        -----------
            race_results (pd.DataFrame): DataFrame containing the race results.
            race_name (str): Name of the race.
        """
        self.race_results = race_results
        self.race_name = race_name

    @staticmethod
    def _get_points_distribution():
        """
        Define the points distribution array.

        Returns:
        --------
            list: Points distribution array.
        """
        return [250, 240] + list(range(230, 170, -5)) + list(range(170, 100, -2)) + list(range(100, 0, -1))

    def calculate_points(self, sort_column='Tijd', position_column=None):
        """
        Calculate points for participants based on race results.

        Parameters:
        -----------
            sort_column (str, optional): Column name to sort the race results. Default is 'Tijd'.
            position_column (str, optional): Column name specifying the position in the classification. Default is None.

        Returns:
        --------
            pd.DataFrame: DataFrame with points assigned to participants.
        """
        # Define the points distribution array
        points_distribution = self._get_points_distribution()

        # Sort race results by the specified column (e.g., final time) and (if needed) by position
        if position_column:
            sorted_results = self.race_results.sort_values(by=[sort_column, position_column])
        else:
            sorted_results = self.race_results.sort_values(by=sort_column)

        # Get the number of participants and calculate the length of the points distribution array
        num_participants = len(sorted_results)
        points_array_length = len(points_distribution)

        # Using minimum of number participants and points_array_length to avoid index out-of-range
        points_to_assign = points_distribution[:min(num_participants, points_array_length)]
        sorted_results[f'Points_{self.race_name}'] = points_to_assign + [1] * (num_participants - len(points_to_assign))

        # Assign ranks based on the sorted order; this automatically considers position_column if provided
        sorted_results[f'Rank_{self.race_name}'] = range(1, num_participants + 1)

        return sorted_results
