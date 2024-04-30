"""Module to calculate the classification"""
import argparse
import pandas as pd
import numpy as np
from classification.reader import ResultReader
from classification.processor import ResultProcessor
from classification.scorer import ResultScorer


# pylint: disable=too-many-statements
def calculate_points_for_year(path, year, races):
    """
    Calculate points for participants based on race results for a given year and list of races.

    Parameters:
    -----------
        path (str): Path to directory with input and output sub-directories for various races
        year (int): Year for which race results are being calculated.
        races (list): List of race names for which results are to be processed.

    Returns:
    --------
        tuple: Two pandas DataFrames with combined results for men and women, containing total points and ranks.
    """
    # Prepare base input path for the given year
    path = path + f'/input/{year}/'

    # Initialise containers for men's and women's data from each race
    race_dfs_men = []
    race_dfs_women = []

    # Process each race to extract and score results
    for race in races:
        reader = ResultReader('excel')
        if 'Sittard' in race:
            df_all = reader.read_results(path + f'{race}/uitslag.xlsx')
            result = ResultProcessor(df_all=df_all)
            place_string = 'Plaats'
        elif 'Borne' in race:
            df_men = reader.read_results(path + f'{race}/Run Bike Run Borne 26-08-2023 Uitslag Overall Mannen.xlsx')
            df_women = reader.read_results(path + f'{race}/Run Bike Run Borne 26-08-2023 Uitslag Overall Vrouwen.xlsx')
            result = ResultProcessor(df_men=df_men, df_women=df_women)
            place_string = None
        elif 'Hulsbeek' in race:
            df_all = reader.read_results(path + f'{race}/Uitslagen_aangepast.xlsx')
            result = ResultProcessor(df_all=df_all)
            place_string = 'uitslag'

        # Process the input
        result.process_results(race, year)

        # Calculate the assigned scores according to race result
        scorer_men = ResultScorer(result.df_men, race)
        scorer_women = ResultScorer(result.df_women, race)
        df_points_men = scorer_men.calculate_points('Tijd', place_string)
        df_points_women = scorer_women.calculate_points('Tijd', place_string)

        # Normalise names to title case
        df_points_men['Naam'] = df_points_men['Naam'].map(str.title)
        df_points_women['Naam'] = df_points_women['Naam'].map(str.title)

        # Append the relevant columns of the DataFrame to the list of race DataFrames
        race_dfs_men.append(df_points_men[['Naam', f'Points_{race}', f'Rank_{race}']])
        race_dfs_women.append(df_points_women[['Naam', f'Points_{race}', f'Rank_{race}']])

    # Merge DataFrames for all races on 'Naam' column
    combined_df_men = race_dfs_men[0]
    for df_men in race_dfs_men[1:]:
        combined_df_men = pd.merge(combined_df_men, df_men, on='Naam', how='outer')

    combined_df_women = race_dfs_women[0]
    for df_women in race_dfs_women[1:]:
        combined_df_women = pd.merge(combined_df_women, df_women, on='Naam', how='outer')

    # Create columns for additional sorting in case of tie's based on highest rank
    for df in [combined_df_men, combined_df_women]:
        rank_columns = df.filter(like='Rank_')
        max_ranks = len(rank_columns.columns)
        for idx in range(max_ranks):
            column_name = f'Top{idx+1}_Rank'
            df[column_name] = rank_columns.apply(
                lambda x, rank=idx+1: x.nsmallest(rank).iloc[-1]
                if x.nsmallest(rank).size > 0 else np.nan,
                axis=1
            )

    # Calculate the total points for each participant as the sum of points of the top 3 scores of all races
    # and add bonus points for participation in 4 and 5 races
    for df in (combined_df_men, combined_df_women):
        race_count = df.filter(regex=r'^Points').gt(0).sum(axis=1)
        df['Bonus'] = 15 * (race_count == 4) + 30 * (race_count == 5)
        df['Total'] = df.filter(regex=r'^Points').apply(lambda x: x.nlargest(3).sum(), axis=1)
        df['Total'] += df['Bonus']

        df['Bonus'] = df['Bonus'].replace(0, -1)

    # Sort the combined results based on total points
    rank_columns = [col for col in combined_df_men.columns if col.startswith('Top') and col.endswith('_Rank')]
    max_ranks = len(rank_columns)
    combined_df_men = combined_df_men.sort_values(by=['Total'] + rank_columns,
                                                  ascending=[False] + [True] * max_ranks)
    combined_df_women = combined_df_women.sort_values(by=['Total'] + rank_columns,
                                                      ascending=[False] + [True] * max_ranks)

    # Calculate rank based on 'Total'. Tie breaker is based on highest rank in individual races
    combined_df_men['Rank'] = combined_df_men.apply(
        lambda row: tuple([row['Total']] + [-row[col] if pd.notna(row[col]) else float('inf') for col in rank_columns]),
        axis=1
    ).rank(method='min', ascending=False).astype(int)
    combined_df_women['Rank'] = combined_df_women.apply(
        lambda row: tuple([row['Total']] + [-row[col] if pd.notna(row[col]) else float('inf') for col in rank_columns]),
        axis=1
    ).rank(method='min', ascending=False).astype(int)

    # Fill NaN values with -1 for now
    combined_df_men = combined_df_men.fillna(-1)
    combined_df_women = combined_df_women.fillna(-1)

    # Convert all numeric columns to integers
    numeric_cols = combined_df_men.select_dtypes(include='number').columns
    combined_df_men[numeric_cols] = combined_df_men[numeric_cols].astype(int)
    combined_df_women[numeric_cols] = combined_df_women[numeric_cols].astype(int)

    # Fill NaN values (represented by -1) with empty string
    combined_df_men[numeric_cols] = combined_df_men[numeric_cols].replace(-1, '').astype(object)
    combined_df_women[numeric_cols] = combined_df_women[numeric_cols].replace(-1, '').astype(object)

    return combined_df_men, combined_df_women


def main():
    """
    Main function to process race results and calculate points.

    Parser arguments:
    -----------
        path (str): Path to directory with input and output sub-directories for various races
        year (int): The year of the races.
        races (list of str): A list containing the names of the races to process.
    """

    parser = argparse.ArgumentParser(description="Process race results and calculate points.")
    parser.add_argument("path", type=str, help="Path to directory with input and output folders")
    parser.add_argument("year", type=int, help="Year of the races")
    parser.add_argument("races", nargs="+", help="List of races")
    args = parser.parse_args()

    # Your code for processing race results and calculating points goes here
    print(f"Processing results for year {args.year} and races: {', '.join(args.races)}")
    results_men, results_women = calculate_points_for_year(args.path, args.year, args.races)

    # Print the final dataframes to quickly check if everything is OK
    print(results_men.to_string())
    print(results_women.to_string())

    points_columns = [col for col in results_men.columns if col.startswith('Points_')]
    columns_needed = ['Naam', 'Rank', 'Total'] + points_columns + ['Bonus']
    print(results_men[columns_needed].to_string(index=False))
    print(results_women[columns_needed].to_string(index=False))

    # Store dataframes in Excel file, to easily copy data in final layout
    outpath = args.path + f'/output/klassement_{args.races[-1]}/'
    results_men[columns_needed].to_excel(f'{outpath}/Klassement_{args.races[-1]}_Man.xlsx', index=False)
    results_women[columns_needed].to_excel(f'{outpath}/Klassement_{args.races[-1]}_Vrouw.xlsx', index=False)


if __name__ == "__main__":
    main()
