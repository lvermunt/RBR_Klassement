# Copyright (c) 2026
"""Module to calculate the classification."""

import argparse

import polars as pl
from loguru import logger

from classification.processor import ResultProcessor
from classification.reader import ResultReader
from classification.scorer import ResultScorer

_BONUS_COUNT_SIX = 6
_BONUS_COUNT_SEVEN = 7


def process_race(path: str, race: str, year: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Process individual race results and return scored DataFrames for men and women."""
    reader = ResultReader("excel")
    place_string: str | None = None
    if "Sittard" in race:
        df_men = reader.read_results(path + f"{race}/RBR_Sittard_18-04-2026_Mannen.xlsx")
        df_women = reader.read_results(path + f"{race}/RBR_Sittard_18-04-2026_Vrouwen.xlsx")
        result = ResultProcessor(df_men=df_men, df_women=df_women)
    elif "Rotterdam" in race:
        df_men = reader.read_results(path + f"{race}/RBR_Rotterdam_28032026_Mannen.xlsx")
        df_women = reader.read_results(path + f"{race}/RBR_Rotterdam_28032026_Vrouwen.xlsx")
        result = ResultProcessor(df_men=df_men, df_women=df_women)
    elif "Almere" in race:
        df_men = reader.read_results(path + f"{race}/RBR_Almere_12042026_Mannen.xlsx")
        df_women = reader.read_results(path + f"{race}/RBR_Almere_12042026_Vrouwen.xlsx")
        result = ResultProcessor(df_men=df_men, df_women=df_women)
    elif "Borne" in race:
        df_men = reader.read_results(path + f"{race}/Run Bike Run Borne 2026 uitslagen voor RBR Series Mannen.xlsx")
        df_women = reader.read_results(path + f"{race}/Run Bike Run Borne 2026 uitslagen voor RBR Series Vrouwen.xlsx")
        result = ResultProcessor(df_men=df_men, df_women=df_women)
        place_string = "Rank"
    elif "Hulsbeek" in race:
        df_men = reader.read_results(
            path + f"{race}/Run Bike Run Oldenzaal 2026 uitslagen voor RBR Series Mannen.xlsx",
        )
        df_women = reader.read_results(
            path + f"{race}/Run Bike Run Oldenzaal 2026 uitslagen voor RBR Series Vrouwen.xlsx",
        )
        result = ResultProcessor(df_men=df_men, df_women=df_women)
        place_string = "Klassering"
    elif "Bathmen" in race:
        df_men = reader.read_results(path + f"{race}/Run Bike Run Bathmen 2026 Mannen.xlsx")
        df_women = reader.read_results(path + f"{race}/Run Bike Run Bathmen 2026 Vrouwen.xlsx")
        result = ResultProcessor(df_men=df_men, df_women=df_women)
    elif "Utrecht" in race:
        df_all = reader.read_results(path + f"{race}/uitslag_DuathlonUtrecht_NTB.xlsx")
        result = ResultProcessor(df_all=df_all)
        place_string = "Positie"
    else:
        msg = "Unsupported race. Not (yet) implemented."
        raise ValueError(msg)

    result.process_results(race, year)

    if result.df_men is None or result.df_women is None:
        msg = f"Race results for {race} are incomplete."
        raise ValueError(msg)
    scorer_men = ResultScorer(result.df_men, race)
    scorer_women = ResultScorer(result.df_women, race)
    df_points_men = scorer_men.calculate_points("Tijd", place_string)
    df_points_women = scorer_women.calculate_points("Tijd", place_string)

    df_points_men = df_points_men.with_columns(pl.col("Naam").cast(pl.String).str.to_titlecase())
    df_points_women = df_points_women.with_columns(pl.col("Naam").cast(pl.String).str.to_titlecase())

    return (
        df_points_men.select(["Naam", f"Points_{race}", f"Rank_{race}"]),
        df_points_women.select(["Naam", f"Points_{race}", f"Rank_{race}"]),
    )


def merge_race_dataframes(race_dfs: list[pl.DataFrame]) -> pl.DataFrame:
    """Merge multiple race dataframes into a single dataframe on the participant name."""
    if not race_dfs:
        msg = "At least one race dataframe is required."
        raise ValueError(msg)

    combined_df = race_dfs[0]
    for df in race_dfs[1:]:
        combined_df = combined_df.join(df, on="Naam", how="full")
    return combined_df


def calculate_ranks_and_totals(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate totals and rank columns for a combined classification dataframe."""
    rank_columns = [column for column in df.columns if column.startswith("Rank_")]
    max_ranks = len(rank_columns)

    for idx in range(max_ranks):
        column_name = f"Top{idx + 1}_Rank"
        values = []
        for row in df.iter_rows(named=True):
            values_in_row = [float(row[col]) for col in rank_columns if row[col] not in (None, "", 0)]
            if not values_in_row:
                values.append(None)
                continue
            values.append(sorted(values_in_row)[idx] if len(values_in_row) > idx else None)
        df = df.with_columns(pl.Series(name=column_name, values=values))

    bonus_values = []
    for row in df.iter_rows(named=True):
        point_columns = [column for column in df.columns if column.startswith("Points_")]
        counted = sum(1 for column in point_columns if row.get(column) not in (None, "", 0))
        if counted == _BONUS_COUNT_SIX:
            bonus_values.append(10)
        elif counted == _BONUS_COUNT_SEVEN:
            bonus_values.append(20)
        else:
            bonus_values.append(0)
    df = df.with_columns(pl.Series(name="Bonus", values=bonus_values))

    total_values = []
    for row in df.iter_rows(named=True):
        point_columns = [column for column in df.columns if column.startswith("Points_")]
        points = [float(row[column]) for column in point_columns if row.get(column) not in (None, "", 0)]
        total_values.append(sum(sorted(points, reverse=True)[:5]) + row["Bonus"])
    df = df.with_columns(pl.Series(name="Total", values=total_values))

    sort_columns = ["Total"] + [f"Top{idx + 1}_Rank" for idx in range(max_ranks)]
    df = df.sort(by=sort_columns, descending=[True] + [False] * max_ranks)
    return df.with_columns(pl.Series(name="Rank", values=list(range(1, df.height + 1))))


def calculate_points_for_year(path: str, year: int, races: list[str]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Calculate points for participants based on race results for a given year."""
    path = path + f"/input/{year}/"
    race_dfs_men: list[pl.DataFrame] = []
    race_dfs_women: list[pl.DataFrame] = []

    for race in races:
        df_points_men, df_points_women = process_race(path, race, year)
        race_dfs_men.append(df_points_men)
        race_dfs_women.append(df_points_women)

    reader = ResultReader("excel")
    df_ag_men = reader.read_results(path + "Agegroups_mannen.xlsx").unique(subset=["Naam"], maintain_order=True)
    df_ag_women = reader.read_results(path + "Agegroups_vrouwen.xlsx").unique(subset=["Naam"], maintain_order=True)
    df_ag_men = df_ag_men.with_columns(pl.col("Naam").cast(pl.String).str.to_titlecase())
    df_ag_women = df_ag_women.with_columns(pl.col("Naam").cast(pl.String).str.to_titlecase())

    combined_df_men = merge_race_dataframes(race_dfs_men)
    combined_df_women = merge_race_dataframes(race_dfs_women)

    combined_df_men = calculate_ranks_and_totals(combined_df_men)
    combined_df_women = calculate_ranks_and_totals(combined_df_women)

    combined_df_men = combined_df_men.sort("Rank")
    combined_df_women = combined_df_women.sort("Rank")

    combined_df_men = combined_df_men.join(df_ag_men, on="Naam", how="left")
    combined_df_women = combined_df_women.join(df_ag_women, on="Naam", how="left")

    missing_men = combined_df_men.filter(pl.col("AgeGroup").is_null()).get_column("Naam").to_list()
    missing_women = combined_df_women.filter(pl.col("AgeGroup").is_null()).get_column("Naam").to_list()
    logger.info("Entries without AgeGroup: {} / {}", missing_men, missing_women)

    rank_ag_men = []
    for group_name, group_df in combined_df_men.group_by("AgeGroup", maintain_order=True):
        ranked = group_df.sort("Total", descending=True)
        rank_map = {row["Naam"]: idx for idx, row in enumerate(ranked.iter_rows(named=True), start=1)}
        items = [f"{rank_map[row['Naam']]} ({group_name})" for row in group_df.iter_rows(named=True)]
        rank_ag_men.extend(items)
    combined_df_men = combined_df_men.with_columns(pl.Series(name="Rank_AG", values=rank_ag_men))

    rank_ag_women = []
    for group_name, group_df in combined_df_women.group_by("AgeGroup", maintain_order=True):
        ranked = group_df.sort("Total", descending=True)
        rank_map = {row["Naam"]: idx for idx, row in enumerate(ranked.iter_rows(named=True), start=1)}
        items = [f"{rank_map[row['Naam']]} ({group_name})" for row in group_df.iter_rows(named=True)]
        rank_ag_women.extend(items)
    combined_df_women = combined_df_women.with_columns(pl.Series(name="Rank_AG", values=rank_ag_women))

    combined_df_men = combined_df_men.fill_null(-1)
    combined_df_women = combined_df_women.fill_null(-1)
    combined_df_men = combined_df_men.with_columns(pl.col("Bonus").replace(0, -1))
    combined_df_women = combined_df_women.with_columns(pl.col("Bonus").replace(0, -1))

    return combined_df_men, combined_df_women


def main() -> None:
    """Process race results and calculate points."""
    parser = argparse.ArgumentParser(description="Process race results and calculate points.")
    parser.add_argument("path", type=str, help="Path to directory with input and output folders")
    parser.add_argument("year", type=int, help="Year of the races")
    parser.add_argument("races", nargs="+", help="List of races")
    args = parser.parse_args()

    logger.info("Processing results for year {} and races: {}", args.year, ", ".join(args.races))
    results_men, results_women = calculate_points_for_year(args.path, args.year, args.races)

    points_columns = [col for col in results_men.columns if col.startswith("Points_")]
    columns_needed = ["Naam", "Rank", "Total", *points_columns, "Bonus", "Rank_AG"]
    logger.info("Final dataframes (selected columns):")
    logger.info("{}", results_men.select(columns_needed).head(10))
    logger.info("{}", results_women.select(columns_needed).head(10))

    outpath = args.path + f"/output/klassement_{args.races[-1]}/"
    results_men.select(columns_needed).write_excel(f"{outpath}/Klassement_{args.races[-1]}_Man.xlsx")
    results_women.select(columns_needed).write_excel(f"{outpath}/Klassement_{args.races[-1]}_Vrouw.xlsx")


if __name__ == "__main__":
    main()
