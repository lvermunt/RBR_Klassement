"""Module to calculate the classification."""
import argparse
import pandas as pd
import numpy as np
from classification.reader import ResultReader
from classification.processor import ResultProcessor
from classification.scorer import ResultScorer


def process_race(path, race, year):
    """
    Process individual race results and return scored DataFrames for men and women.

    Parameters:
    -----------
        path (str): Path to directory with input and output sub-directories for the race
        race (str): Race names for which results are to be processed.
        year (int): Year for which race results are being calculated.

    Returns:
    --------
        tuple: Two pandas DataFrames with the results for men and women, containing total points and ranks.
    """
    reader = ResultReader('excel')
    place_string = None
    if 'Sittard' in race:
        df_all = reader.read_results(path + f'{race}/uitslag.xlsx')
        result = ResultProcessor(df_all=df_all)
        place_string = 'Plaats'
    elif 'Borne' in race:
        df_men = reader.read_results(path + f'{race}/Run Bike Run Borne 24-08-2024 Uitslag Overall Mannen.xlsx')
        df_women = reader.read_results(path + f'{race}/Run Bike Run Borne 24-08-2024 Uitslag Overall Vrouwen.xlsx')
        result = ResultProcessor(df_men=df_men, df_women=df_women)
    elif 'Hulsbeek' in race:
        df_all = reader.read_results(path + f'{race}/Uitslagen_aangepast.xlsx')
        result = ResultProcessor(df_all=df_all)
        place_string = 'uitslag'
    elif 'Bathmen' in race:
        df_men = reader.read_results(path + f'{race}/Uitslag_man_extracted.xlsx')
        df_women = reader.read_results(path + f'{race}/Uitslag_vrouw_extracted.xlsx')
        result = ResultProcessor(df_men=df_men, df_women=df_women)
        place_string = 'POS'
    elif 'Utrecht' in race:
        df_all = reader.read_results(path + f'{race}/uitslag_RBRUtrecht.xlsx')
        result = ResultProcessor(df_all=df_all)
        place_string = 'Positie'
    else:
        raise ValueError("Unsupported race. Not (yet) implemented.")

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

    # Return the relevant columns of the DataFrames
    return df_points_men[['Naam', f'Points_{race}', f'Rank_{race}']], \
        df_points_women[['Naam', f'Points_{race}', f'Rank_{race}']]


def merge_race_dataframes(race_dfs):
    """
    Merge dataframes from multiple races into a single dataframe.

    Parameters:
    -----------
        race_dfs (list of pd.DataFrame): A list of dataframes, each representing results from different races.

    Returns:
    --------
        pd.DataFrame: A single dataframe merged from all input race dataframes on 'Naam'.
    """
    combined_df = race_dfs[0]
    for df in race_dfs[1:]:
        combined_df = pd.merge(combined_df, df, on='Naam', how='outer')
    return combined_df


def calculate_ranks_and_totals(df):
    """
    Calculate and assign ranks and totals for the combined dataframe.

    This function adds a 'Bonus' for participants based on their participation in multiple races,
    calculates 'Total' points considering the top 3 scores, sorts by 'Total' points and rank tier, and
    assigns a final rank considering tie-breaking rules based on highest individual race ranks.

    Parameters:
    -----------
        df (pd.DataFrame): DataFrame containing points and ranks from multiple races for participants.
    """
    # Create columns for additional sorting in case of tie's based on highest rank
    rank_columns = df.filter(like='Rank_')
    max_ranks = len(rank_columns.columns)
    for idx in range(max_ranks):
        column_name = f'Top{idx + 1}_Rank'
        df[column_name] = rank_columns.apply(
            lambda x, rank=idx + 1: x.nsmallest(rank).iloc[-1]
            if x.nsmallest(rank).size > 0 else np.nan,
            axis=1
        )

    # Calculate the total points for each participant as the sum of points of the top 3 scores of all races
    # and add bonus points for participation in 4 and 5 races
    race_count = df.filter(regex=r'^Points').gt(0).sum(axis=1)
    df['Bonus'] = 15 * (race_count == 4) + 30 * (race_count == 5)
    df['Total'] = df.filter(regex=r'^Points').apply(lambda x: x.nlargest(3).sum(), axis=1)
    df['Total'] += df['Bonus']

    # Sort the combined results based on total points
    sort_col = ['Total'] + [f'Top{idx + 1}_Rank' for idx in range(max_ranks)]
    df = df.sort_values(by=sort_col, ascending=[False] + [True] * max_ranks)

    # Calculate rank based on 'Total'. Tie breaker is based on highest rank in individual races
    df['Rank'] = df.apply(
        lambda row: tuple([row['Total']] + [-row[col] if pd.notna(row[col]) else float('inf') for col in sort_col[1:]]),
        axis=1
    ).rank(method='min', ascending=False).astype(int)

    return df


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
    path = path + f'/input/{year}/'
    race_dfs_men, race_dfs_women = [], []

    for race in races:
        df_points_men, df_points_women = process_race(path, race, year)
        race_dfs_men.append(df_points_men)
        race_dfs_women.append(df_points_women)

    # Load agegroup information
    reader = ResultReader('excel')
    df_ag_men = reader.read_results(path + 'Agegroups_mannen.xlsx')
    df_ag_women = reader.read_results(path + 'Agegroups_vrouwen.xlsx')
    df_ag_men['Naam'] = df_ag_men['Naam'].map(str.title)
    df_ag_women['Naam'] = df_ag_women['Naam'].map(str.title)

    combined_df_men = merge_race_dataframes(race_dfs_men)
    combined_df_women = merge_race_dataframes(race_dfs_women)

    combined_df_men = calculate_ranks_and_totals(combined_df_men)
    combined_df_women = calculate_ranks_and_totals(combined_df_women)

    # Sort DataFrames on rank, which takes ties properly into account
    combined_df_men = combined_df_men.sort_values(by=['Rank'])
    combined_df_women = combined_df_women.sort_values(by=['Rank'])

    # Add AG information and calculate ranking
    combined_df_men = pd.merge(combined_df_men, df_ag_men, on='Naam', how='left')
    combined_df_women = pd.merge(combined_df_women, df_ag_women, on='Naam', how='left')
    combined_df_men['Rank_AG'] = combined_df_men.groupby('AgeGroup')['Total'].rank(method='min', ascending=False)
    combined_df_women['Rank_AG'] = combined_df_women.groupby('AgeGroup')['Total'].rank(method='min', ascending=False)
    combined_df_men["Rank_AG"] = combined_df_men["Rank_AG"].astype(int).astype(str) + \
        ' (' + combined_df_men['AgeGroup'] + ')'
    combined_df_women["Rank_AG"] = combined_df_women["Rank_AG"].astype(int).astype(str) + \
        ' (' + combined_df_women['AgeGroup'] + ')'

    # Fill NaN values with -1 for now
    combined_df_men = combined_df_men.fillna(-1)
    combined_df_women = combined_df_women.fillna(-1)
    combined_df_men['Bonus'] = combined_df_men['Bonus'].replace(0, -1)
    combined_df_women['Bonus'] = combined_df_women['Bonus'].replace(0, -1)

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
    columns_needed = ['Naam', 'Rank', 'Total'] + points_columns + ['Bonus', 'Rank_AG']
    print(results_men[columns_needed].to_string(index=False))
    print(results_women[columns_needed].to_string(index=False))

    # Store dataframes in Excel file, to easily copy data in final layout
    outpath = args.path + f'/output/klassement_{args.races[-1]}/'
    results_men[columns_needed].to_excel(f'{outpath}/Klassement_{args.races[-1]}_Man.xlsx', index=False)
    results_women[columns_needed].to_excel(f'{outpath}/Klassement_{args.races[-1]}_Vrouw.xlsx', index=False)


if __name__ == "__main__":
    main()
