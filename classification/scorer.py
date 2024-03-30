"""Module to assign points to the participants based on race results"""

class ResultScorer:
    """
    A class to assign points to participants based on race results.
    """

    def __init__(self, race_results):
        """
        Initialize the ResultScorer with race results DataFrame.

        Parameters:
        -----------
        race_results : pd.DataFrame
            DataFrame containing the race results.
        """
        self.race_results = race_results

    def calculate_points(self, sort_column='Tijd'):
        """
        Calculate points for participants based on race results.

        Parameters:
        -----------
        sort_column : str, optional
            Column name to sort the race results. Default is 'Tijd'.

        Returns:
        --------
        pd.DataFrame
            DataFrame with points assigned to participants.
        """
        # Define the points distribution array
        points_distribution = [
            250, 240, 230, 225, 220, 215, 210, 205, 200, 195, 190, 185, 180, 175, 170,
            168, 166, 164, 162, 160, 158, 156, 154, 152, 150, 148, 146, 144, 142, 140,
            138, 136, 134, 132, 130, 128, 126, 124, 122, 120, 118, 116, 114, 112, 110,
            108, 106, 104, 102, 100, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87,
            86, 85, 84, 83, 82, 81, 80, 79, 78, 77, 76, 75, 74, 73, 72, 71, 70, 69, 68,
            67, 66, 65, 64, 63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49,
            48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31, 30,
            29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11,
            10, 9, 8, 7, 6, 5, 4, 3, 2, 1
        ]

        # Sort race results by the specified column
        sorted_results = self.race_results.sort_values(by=sort_column)

        # Get the number of participants and calculate the length of the points distribution array
        num_participants = len(sorted_results)
        points_array_length = len(points_distribution)

        # Calculate points based on position
        if num_participants <= points_array_length:
            sorted_results['Points'] = points_distribution[:num_participants]
        else:
            sorted_results['Points'] = points_distribution[:points_array_length]
            # Assign 1 point to participants with a ranking exceeding the length of points_distribution array
            sorted_results.loc[points_array_length:, 'Points'] = 1

        return sorted_results
