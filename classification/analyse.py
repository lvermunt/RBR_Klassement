"""Module to calculate the classification"""
import argparse
import pandas as pd
from classification.reader import ResultReader
from classification.processor import ResultProcessor
from classification.scorer import ResultScorer

def calculate_points_for_year(year, races):
    """
    Calculate points for participants based on race results for a given year and list of races.

    Parameters:
    -----------
    year : int
        Year for which race results are being calculated.
    races : list
        List of race names for which results are to be processed.

    Returns:
    --------
    pd.DataFrame
        DataFrame with combined results containing total points and ranks.
    """

    # Define the path to the race results directory for the given year
    # TODO: No hardcoded path
    path = f'/Users/lvermunt/Documents/Tijdelijk/Persoonlijk/PersonalProjects/RunBikeRunSeries/input/{year}/'

    # Initialise an empty list to store DataFrames for each race
    race_dfs_men = []
    race_dfs_women = []

    # Iterate over each race in the list and process its results
    for race in races:
        if 'Sittard' in race:
            reader = ResultReader('excel')
            df_all = reader.read_results(path + f'{race}/uitslag.xlsx')
            result = ResultProcessor(df_all=df_all)
            time_string = 'Tijd'
            place_string = 'Plaats'
        elif 'Borne' in race:
            reader = ResultReader('excel')
            df_men = reader.read_results(path + f'{race}/Run Bike Run Borne 26-08-2023 Uitslag Overall Mannen.xlsx')
            df_women = reader.read_results(path + f'{race}/Run Bike Run Borne 26-08-2023 Uitslag Overall Vrouwen.xlsx')
            result = ResultProcessor(df_men=df_men, df_women=df_women)
            time_string = 'Totaal'
            place_string = None

        result.process_results(race, year)
        scorer_men = ResultScorer(result.df_men, race)
        scorer_women = ResultScorer(result.df_women, race)
        df_points_men = scorer_men.calculate_points(time_string, place_string)
        df_points_women = scorer_women.calculate_points(time_string, place_string)
        df_points_men['Naam'] = df_points_men['Naam'].map(str.title)
        df_points_women['Naam'] = df_points_women['Naam'].map(str.title)

        # Append the relevant columns of the DataFrame to the list of race DataFrames
        race_dfs_men.append(df_points_men[['Naam', f'Points_{race}']])
        race_dfs_women.append(df_points_women[['Naam', f'Points_{race}']])

    # Merge DataFrames for all races on 'Naam' column
    combined_df_men = race_dfs_men[0]
    for df_men in race_dfs_men[1:]:
        combined_df_men = pd.merge(combined_df_men, df_men, on='Naam', how='outer')
    combined_df_women = race_dfs_women[0]
    for df_women in race_dfs_women[1:]:
        combined_df_women = pd.merge(combined_df_women, df_women, on='Naam', how='outer')

    # Fill NaN values with zero
    combined_df_men = combined_df_men.fillna(0)
    combined_df_women = combined_df_women.fillna(0)

    # Calculate the total points for each participant as the sum of points from all races
    combined_df_men = combined_df_men.copy()
    combined_df_men['Points_Total'] = combined_df_men.select_dtypes(include='number').sum(axis=1, skipna=True)
    combined_df_women = combined_df_women.copy()
    combined_df_women['Points_Total'] = combined_df_women.select_dtypes(include='number').sum(axis=1, skipna=True)
    #TODO: This should be the maximum of 3 of the 5 races

    # TODO: Visual check if there are no duplicates in name because of typos
    # TODO: Ask for confirmation before continuing

    #TODO: Assign bonus points in case of 4 or 5 races

    # Sort the combined results based on total points
    combined_df_men = combined_df_men.sort_values(by='Points_Total', ascending=False)
    combined_df_women = combined_df_women.sort_values(by='Points_Total', ascending=False)

    # Calculate rank based on 'Points_Total'
    combined_df_men['Rank'] = combined_df_men['Points_Total'].rank(ascending=False, method='min').astype(int)
    combined_df_women['Rank'] = combined_df_women['Points_Total'].rank(ascending=False, method='min').astype(int)

    return combined_df_men, combined_df_women

def main(year, races):
    """
    Process race results and calculate points for the specified year and races.

    Parameters:
    -----------
    year : int
        The year of the races.
    races : list of str
        A list containing the names of the races to process.
    """

    # Your code for processing race results and calculating points goes here
    print(f"Processing results for year {year} and races: {', '.join(races)}")

    results_men, results_women = calculate_points_for_year(year, races)

    #TODO: Print in a way so it can directly be copied inside excel sheet
    print(results_men.to_string())
    print(results_women.to_string())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process race results and calculate points.")
    parser.add_argument("year", type=int, help="Year of the races")
    parser.add_argument("races", nargs="+", help="List of races")
    args = parser.parse_args()

    main(args.year, args.races)
