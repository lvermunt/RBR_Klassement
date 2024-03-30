from classification.reader import ResultReader
from classification.processor import ResultProcessor
from classification.scorer import ResultScorer
import pandas as pd

path = '/Users/lvermunt/Documents/Tijdelijk/Persoonlijk/PersonalProjects/RunBikeRunSeries/input/2023/'

reader_sittard = ResultReader('excel')
df_all_sittard = reader_sittard.read_results(path + 'Sittard/uitslag.xlsx')
reader_borne = ResultReader('excel')
df_men_borne = reader_borne.read_results(path + 'Borne/Run Bike Run Borne 26-08-2023 Uitslag Overall Mannen.xlsx')
df_women_borne = reader_borne.read_results(path + 'Borne/Run Bike Run Borne 26-08-2023 Uitslag Overall Vrouwen.xlsx')

result_sittard = ResultProcessor(df_all=df_all_sittard)
result_sittard.process_results('Sittard')
result_borne = ResultProcessor(df_men=df_men_borne, df_women=df_women_borne)
result_borne.process_results('Borne')

scorer_sittard_men = ResultScorer(result_sittard.df_men)
scorer_sittard_women = ResultScorer(result_sittard.df_women)
df_points_sittard_men = scorer_sittard_men.calculate_points('Tijd')
df_points_sittard_women = scorer_sittard_women.calculate_points('Tijd')
scorer_borne_men = ResultScorer(result_borne.df_men)
scorer_borne_women = ResultScorer(result_borne.df_women)
df_points_borne_men = scorer_borne_men.calculate_points('Totaal')
df_points_borne_women = scorer_borne_women.calculate_points('Totaal')

df_sittard_men = df_points_sittard_men[['Naam', 'Points']]
df_sittard_women = df_points_sittard_women[['Naam', 'Points']]
df_borne_men = df_points_borne_men[['Naam', 'Points']]
df_borne_women = df_points_borne_women[['Naam', 'Points']]

df_sittard_men['Naam'] = df_sittard_men['Naam'].map(str.title)
df_sittard_women['Naam'] = df_sittard_women['Naam'].map(str.title)
df_borne_men['Naam'] = df_borne_men['Naam'].map(str.title)
df_borne_women['Naam'] = df_borne_women['Naam'].map(str.title)

# Merge dataframes on 'Naam' column
df_men_combi = pd.merge(df_sittard_men, df_borne_men, on='Naam', how='outer', suffixes=('_Sittard', '_Borne'))

# Fill NaN values with empty string
df_men_combi.fillna(0, inplace=True)

# Calculate the sum of points from both races
df_men_combi['Points_Total'] = df_men_combi['Points_Sittard'] + df_men_combi['Points_Borne']

# TODO: Visual check if there are no duplicates in name because of typos
# TODO: Ask for confirmation before continuing

df_men_combi = df_men_combi.sort_values(by='Points_Total', ascending=False)

# Calculate rank based on 'Points_Total'
df_men_combi['Rank'] = df_men_combi['Points_Total'].rank(ascending=False, method='min').astype(int)

print(df_men_combi.to_string())