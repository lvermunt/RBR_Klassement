# Copyright (c) 2026
"""Module to assign points to participants based on race results."""

import polars as pl


class ResultScorer:
    """A class to assign points to participants based on race results."""

    def __init__(self, race_results: pl.DataFrame | None, race_name: str) -> None:
        """Initialise the ResultScorer with race results DataFrame."""
        if race_results is None:
            msg = "Race results cannot be None."
            raise ValueError(msg)
        self.race_results = race_results
        self.race_name = race_name

    @staticmethod
    def _get_points_distribution() -> list[int]:
        """Define the points distribution array."""
        return [
            *[250, 240],
            *list(range(230, 170, -5)),
            *list(range(170, 100, -2)),
            *list(range(100, 0, -1)),
        ]

    def calculate_points(self, sort_column: str = "Tijd", position_column: str | None = None) -> pl.DataFrame:
        """Calculate points for participants based on race results."""
        points_distribution = self._get_points_distribution()

        if position_column:
            sorted_results = self.race_results.sort(by=[sort_column, position_column])
        else:
            sorted_results = self.race_results.sort(by=sort_column)

        num_participants = sorted_results.height
        points_to_assign = points_distribution[: min(num_participants, len(points_distribution))]
        points = points_to_assign + [1] * (num_participants - len(points_to_assign))

        return sorted_results.with_columns(
            pl.Series(name=f"Points_{self.race_name}", values=points),
            pl.Series(name=f"Rank_{self.race_name}", values=list(range(1, num_participants + 1))),
        )

