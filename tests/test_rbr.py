"""Module used to test the RBR classification functionalities"""
import unittest
import pandas as pd
from classification.reader import ResultReader
from classification.processor import ResultProcessor
from classification.scorer import ResultScorer

class TestRBR(unittest.TestCase):
    """
    Test cases for the RunBikeRun package.
    """

    def test_read_results(self):
        """
        Testing the functionalities for reading the results
        """

        path = 'tests/input/'
        reader = ResultReader('excel')
        df_all = reader.read_results(path + 'Sittard/uitslag.xlsx')
        self.assertIsInstance(df_all, pd.DataFrame)
        self.assertTrue(not df_all.empty)

    def test_process_results(self):
        """
        Testing the functionalities for processing the results
        """

        path = 'tests/input/'
        reader = ResultReader('excel')
        df_all = reader.read_results(path + 'Sittard/uitslag.xlsx')
        processor = ResultProcessor(df_all=df_all)
        processor.process_results('Sittard', year=2023)
        #TODO: Add assertions to check if processing is done correctly

    def test_calculate_points(self):
        """
        Testing the functionalities for calculating the scores
        """

        # Test calculating points for race results
        path = 'tests/input/'
        reader = ResultReader('excel')
        df_all = reader.read_results(path + 'Sittard/uitslag.xlsx')
        processor = ResultProcessor(df_all=df_all)
        processor.process_results('Sittard', year=2023)
        scorer = ResultScorer(processor.df_men, 'Sittard')
        df_points = scorer.calculate_points(sort_column='Tijd')
        #TODO: Add assertions to check if points calculation is correct

if __name__ == '__main__':
    unittest.main()
