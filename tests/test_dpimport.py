from unittest.mock import patch
import dpimport


@patch("pymongo.collection")
class TestDPImportDataInsert:
    def test_insert_reference(self, mock_collection):
        mock_collection.insert_one.return_value.inserted_id = 1
        assert (
            dpimport.insert_reference(
                mock_collection,
                {
                    "extension": ".csv",
                    "glob": "/directory/study_metadata.csv",
                    "role": "metadata",
                    "study": "study",
                },
            )
            == 1
        )

    @patch("dpimport.prepare_data")
    @patch("tools.reader.read_csv")
    def test_prepare_data(
        self,
        mock_csv_reader,
        mock_prepare_data,
        mock_collection,
    ):
        file_info = {
            "assessment": "assessment",
            "end": "7",
            "extension": ".csv",
            "glob": "/directory/study-subject-assessment-day*.csv",
            "role": "data",
            "start": "3",
            "study": "study",
            "subject": "subject",
            "time_end": 7,
            "time_start": 3,
            "time_units": "day",
            "units": "day",
            "path": "/directory",
        }
        query = {
            "assessment": "assessment",
            "study": "study",
            "subject": "subject",
        }
        mock_collection().find_one().return_value = None

        mock_prepare_data.return_value = {
            "new_data": [
                {
                    "assessment": "assessment",
                    "day": "3",
                    "study": "study",
                    "subject": "subject",
                    "var1": "value1",
                    "var2": "value2",
                },
                {
                    "assessment": "assessment",
                    "day": "7",
                    "study": "study",
                    "subject": "subject",
                    "varA": "valueA",
                    "varB": "valueB",
                },
            ],
            "updated_data": [
                {
                    "assessment": "assessment",
                    "day": "4",
                    "study": "study",
                    "subject": "subject",
                    "var_c": "valueC",
                    "var_d": "valueD",
                },
            ],
        }

        assert mock_csv_reader.to_have_been_called
        assert mock_collection.to_have_been_called
        assert dpimport.prepare_data(mock_collection, file_info, query) == {
            "new_data": [
                {
                    "assessment": "assessment",
                    "day": "3",
                    "study": "study",
                    "subject": "subject",
                    "var1": "value1",
                    "var2": "value2",
                },
                {
                    "assessment": "assessment",
                    "day": "7",
                    "study": "study",
                    "subject": "subject",
                    "varA": "valueA",
                    "varB": "valueB",
                },
            ],
            "updated_data": [
                {
                    "assessment": "assessment",
                    "day": "4",
                    "study": "study",
                    "subject": "subject",
                    "var_c": "valueC",
                    "var_d": "valueD",
                }
            ],
        }
