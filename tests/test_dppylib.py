from unittest.mock import patch
import dppylib


class TestDppyLib:
    def test_guess_type(self):
        assert dppylib.guess_type("path.csv") == ("text/csv", None)

    def test_scan_metadata_file_format_and_return_file_info(self):
        file_name = "study_metadata.csv"
        matched_file = dppylib.METADATA_REGEX.match(file_name)

        assert dppylib.scan_metadata(matched_file, file_name, "/directory") == {
            "extension": ".csv",
            "glob": "/directory/study_metadata.csv",
            "role": "metadata",
            "study": "study",
        }

    def test_scan_data_file_format_and_return_file_info(self):
        file_name = "study-subject-assessment-day3to7.csv"
        matched_file = dppylib.FILE_REGEX.match(file_name)

        assert dppylib.scan_data(matched_file, file_name, "/directory") == {
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
        }

    def test_match_file(self):
        file_name = "some_filename.txt"
        sub_dir = "/directory"
        assert dppylib.match_file(file_name, sub_dir) == None

        file_name = "study-subject-assessment-day3to7.csv"

        assert dppylib.match_file(file_name, sub_dir) == {
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
        }


@patch("pymongo.collection")
class TestDppyLibDataInserts:
    def test_insert_reference(self, mock_collection):
        mock_collection.insert_one.return_value.inserted_id = 1
        assert (
            dppylib.insert_reference(
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

    @patch("dppylib.prepare_data")
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
        assert dppylib.prepare_data(mock_collection, file_info, query) == {
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
