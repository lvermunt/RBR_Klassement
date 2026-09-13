# Copyright (c) 2026
"""Module to process the race results."""

import polars as pl
from loguru import logger

_UTRECHT_2024_YEAR = 2024
_UTRECHT_2025_YEAR = 2025
_TIME_PART_COUNT = 2


class ResultProcessor:
    """Process race results stored in Polars DataFrames."""

    def __init__(
        self,
        df_all: pl.DataFrame | None = None,
        df_men: pl.DataFrame | None = None,
        df_women: pl.DataFrame | None = None,
    ) -> None:
        """Initialize the processor with one or more result frames."""
        self.df_all = df_all
        self.df_men = df_men
        self.df_women = df_women

        if (df_all is None and df_men is None) or (df_all is None and df_women is None):
            msg = "Either a combined or gender-separated dataframe required!"
            raise ValueError(msg)

    def process_results(self, race: str, year: int = 2024) -> None:
        """Process results for a specific race and year."""
        try:
            processing_method = getattr(self, f"process_results_{race.lower()}")
            processing_method(year)
        except AttributeError:
            logger.error('Race "%s" has not been implemented yet', race)

    def process_results_borne(self, _year: int = 2026) -> None:
        """Process the Borne race format."""
        self._process_header(-1, "Time", "Name")
        self._clean_dataframe()

    def process_results_rotterdam(self, _year: int = 2026) -> None:
        """Process the Rotterdam race format."""
        self._process_header(0, "GUN TIME", "NAME")
        self._clean_dataframe()

    def process_results_almere(self, _year: int = 2026) -> None:
        """Process the Almere race format."""
        self._process_header(-1, "GUN TIME", "NAME")
        self._clean_dataframe()

    def process_results_sittard(self, _year: int = 2026) -> None:
        """Process the Sittard race format."""
        self._process_header(-1, "TIME", "Name")
        if self.df_all is not None and self.df_men is None and self.df_women is None:
            self.df_men = self.df_all.clone()
            self.df_women = self.df_all.clone()
        self._clean_dataframe()

    def process_results_hulsbeek(self, _year: int = 2026) -> None:
        """Process the Hulsbeek race format."""
        self._process_header(-1, "Finish", "Naam deelnemer")
        self._clean_dataframe()

    def process_results_bathmen(self, _year: int = 2026) -> None:
        """Process the Bathmen race format."""
        self._process_header(0, "GUN TIME", "NAME")
        self._clean_dataframe()

    def process_results_utrecht(self, year: int = 2025) -> None:
        """Process the Utrecht race format."""
        self._process_header(-1, "Eindtijd", "Deelnemer")
        if self.df_all is None:
            msg = "Utrecht results require a combined dataframe input."
            raise ValueError(msg)
        df_all = self.df_all

        self.df_men = df_all.filter(pl.col("`m/v`") == "m")
        if year == _UTRECHT_2024_YEAR:
            self.df_men = self.df_men.filter(pl.col("Wedstrijd").is_in(["86310 (E+R M)", "86327 (R)"]))
        elif year == _UTRECHT_2025_YEAR:
            self.df_men = self.df_men.filter(pl.col("Wedstrijd").is_in(["90782 (E M)", "90790 (R M)"]))

        self.df_women = df_all.filter(pl.col("`m/v`") == "v")
        if year == _UTRECHT_2024_YEAR:
            self.df_women = self.df_women.filter(pl.col("Wedstrijd").is_in(["86323 (E+R V)", "86327 (R)"]))
        elif year == _UTRECHT_2025_YEAR:
            self.df_women = self.df_women.filter(pl.col("Wedstrijd").is_in(["90786 (E+R V)"]))

        self.df_men = self.df_men.drop("Wedstrijd")
        self.df_women = self.df_women.drop("Wedstrijd")
        self._clean_dataframe()

    def _update_dataframe(self, key: str, df: pl.DataFrame | None) -> None:
        """Set the dataframe for the selected key."""
        if key == "all":
            self.df_all = df
        elif key == "men":
            self.df_men = df
        elif key == "women":
            self.df_women = df

    def _resolve_header_index(self, frame: pl.DataFrame, index_row: int) -> int:
        """Find the row that contains the actual result headers."""
        if index_row != -1:
            return index_row

        for row_idx in range(frame.height):
            row_values = [value for value in frame.row(row_idx) if value is not None]
            if any(str(value).strip() in {"Naam", "Name", "Deelnemer"} for value in row_values):
                return row_idx
        return index_row

    def _rename_target_column(self, frame: pl.DataFrame, candidates: list[str], target: str) -> pl.DataFrame:
        """Rename the selected target column if it exists."""
        for candidate in candidates:
            if candidate in frame.columns:
                return frame.rename({candidate: target})
        return frame

    def _process_header(
        self,
        index_row: int,
        time_column_name: str | None = None,
        participant_column_name: str | None = None,
    ) -> None:
        """Rename the result table headers to the project's canonical names."""
        dataframes = {"all": self.df_all, "men": self.df_men, "women": self.df_women}
        for key, current_frame in dataframes.items():
            if current_frame is None:
                continue

            processed_frame = current_frame
            header_index = self._resolve_header_index(processed_frame, index_row)
            if header_index >= 0:
                header_row = [None if value is None else str(value) for value in processed_frame.row(header_index)]
                new_frame = processed_frame.clone()
                new_frame.columns = [
                    f"column_{idx}" if value in (None, "") else value for idx, value in enumerate(header_row)
                ]
                processed_frame = new_frame.slice(header_index + 1)

            if time_column_name:
                processed_frame = self._rename_target_column(
                    processed_frame,
                    [
                        time_column_name,
                        "Tijd",
                        "Time",
                        "GUN TIME",
                        "Eindtijd",
                        "Finish",
                        "TIME",
                    ],
                    "Tijd",
                )

            if participant_column_name:
                processed_frame = self._rename_target_column(
                    processed_frame,
                    [
                        participant_column_name,
                        "Naam",
                        "Name",
                        "Deelnemer",
                        "Participant",
                    ],
                    "Naam",
                )

            self._update_dataframe(key, processed_frame)

    def _clean_dataframe(self, status_column: int = 0) -> None:
        """Remove incomplete or invalid rows from the processed dataframe."""
        dataframes = {"all": self.df_all, "men": self.df_men, "women": self.df_women}
        for key, current_frame in dataframes.items():
            if current_frame is None:
                continue

            processed_frame = current_frame
            if "Naam" in processed_frame.columns and "Tijd" in processed_frame.columns:
                processed_frame = processed_frame.filter(
                    pl.col("Naam").is_not_null() & pl.col("Tijd").is_not_null(),
                )

            if processed_frame.columns:
                first_column = processed_frame.columns[0]
                processed_frame = processed_frame.filter(
                    ~pl.col(first_column).is_in(["Plaats", "Naam", "Deelnemer", "Name"]),
                )

            if processed_frame.columns and status_column < processed_frame.width:
                status_field = processed_frame.columns[status_column]
                processed_frame = processed_frame.filter(
                    ~pl.col(status_field).is_in(["DQ", "DSQ", "DNS", "DNF"]),
                )

            if "Naam" in processed_frame.columns:
                processed_frame = processed_frame.unique(subset=["Naam"], maintain_order=True)

            self._update_dataframe(key, processed_frame)

    @staticmethod
    def _preprocess_time(time_str: str) -> str:
        """Normalize time values for sorting."""
        parts = time_str.split(":")
        if len(parts) == _TIME_PART_COUNT:
            return "0:" + time_str
        return time_str
