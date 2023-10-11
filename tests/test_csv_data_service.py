from unittest import TestCase
from scripts.csv_data_service.csv_data_service import CSVDataService


class TestCsvData(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.max_columns = 6
        cls.max_days = 5
        cls.participants = 5

    def setUp(self):
        self.csv_generator = CSVDataService(
            self.max_columns, self.max_days, self.participants
        )

    def test_generate_column_data(self):
        self.csv_generator._generate_columns()
        assert len(self.csv_generator.columns) == self.max_columns
        self.csv_generator._generate_column_data()
        assert len(self.csv_generator.day_data) == self.max_columns
