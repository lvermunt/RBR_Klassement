# Copyright (c) 2026
"""Module used to test the RBR classification functionalities."""

import unittest

import polars as pl

from classification.processor import ResultProcessor
from classification.reader import ResultReader
from classification.scorer import ResultScorer


class TestRBR(unittest.TestCase):
    """Test cases for the Run-Bike-Run package."""

    def test_read_results(self) -> None:
        """Test the reading functionality for results."""
        path = "tests/input/"
        reader = ResultReader("excel")
        df_all = reader.read_results(path + "Sittard/uitslag.xlsx")
        if not isinstance(df_all, pl.DataFrame):
            msg = f"Expected pl.DataFrame, got {type(df_all)!r}"
            raise TypeError(msg)
        if df_all.is_empty():
            msg = "Expected a non-empty DataFrame."
            raise ValueError(msg)

    def test_process_results(self) -> None:
        """Test the processing functionality for results."""
        path = "tests/input/"
        reader = ResultReader("excel")
        df_all = reader.read_results(path + "Sittard/uitslag.xlsx")
        processor = ResultProcessor(df_all=df_all)
        processor.process_results("Sittard", year=2023)

    def test_calculate_points(self) -> None:
        """Test the score calculation functionality."""
        path = "tests/input/"
        reader = ResultReader("excel")
        df_all = reader.read_results(path + "Sittard/uitslag.xlsx")
        processor = ResultProcessor(df_all=df_all)
        processor.process_results("Sittard", year=2023)
        if processor.df_men is None:
            msg = "Processor did not populate men results for Sittard."
            raise AssertionError(msg)
        scorer = ResultScorer(processor.df_men, "Sittard")
        scorer.calculate_points(sort_column="Tijd")


if __name__ == "__main__":
    unittest.main()
