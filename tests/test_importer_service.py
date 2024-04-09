from unittest import TestCase
from unittest.mock import patch
import pprint
from scripts.data_importer_service.data_import_service import DataImporterService


class TestImporter(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_file = {}
        cls.metadata_file = {}

    def setUp(self):
        self.importer = DataImporterService(self.data_file, self.metadata_file)

    def test_file_divergence(self):
        path = "blah.csv"

        assert self.importer.process_file(path) == None

        with patch.object(
            self.importer, "process_data_file"
        ) as mocked_process_data_file:
            data_file_path = "study-participant-assessment-day1to4.csv"

            self.importer.process_file(data_file_path)
            mocked_process_data_file.assert_called_once()

        with patch.object(
            self.importer, "process_metadata_file"
        ) as mocked_process_metadata_file:
            metadata_file_path = "site_metadata.csv"

            self.importer.process_file(metadata_file_path)
            mocked_process_metadata_file.assert_called_once()

    def test_processed_data_to_json(self):
        data_file_path = "study-participant-assessment-day1to4.csv"
        data_file_extension_to_dict = self.importer.DATAFILE.match(
            data_file_path
        ).groupdict()
        mock_data_file_info = {
            "path": "study-participant-assessment-day1to4.csv",
            "filetype": "text/csv",
            "encoding": "utf-8",
            "basename": "study-participant-assessment-day1to4.csv",
            "dirname": "/path/to/files",
            "mtime": 1234567890.0,
            "size": 1024,
            "uid": 1000,
            "gid": 1000,
            "mode": 0o644,
        }
        participant_assessments = [
            {
                "var1": 1,
                "var2": 2,
                "var3": "str",
            },
            {
                "var1": 2,
                "var2": 2,
                "var3": "str",
                "var4": 5,
                "var6": 6,
                "var7": "str2",
            },
        ]
        metadata_file_path = "site_metadata.csv"
        metadata_file_extension_to_dict = self.importer.METADATA.match(
            metadata_file_path
        ).groupdict()
        mock_file_info = {
            "filetype": "text/csv",
            "encoding": "utf-8",
            "dirname": "/path/to/files",
            "mtime": 1234567890.0,
            "size": 1024,
            "uid": 1000,
            "gid": 1000,
            "mode": 0o644,
        }
        participants = [
            {
                "Subject ID": "YA1",
                "Active": 1,
                "Consent": "2022-06-02",
                "Study": "YA",
            },
            {
                "Subject ID": "YA2",
                "Active": 1,
                "Consent": "2022-06-02",
                "Study": "YA",
            },
        ]

        with patch.object(
            self.importer, "_file_info", return_value=mock_data_file_info
        ), patch.object(
            self.importer, "_read_csv", return_value=participant_assessments
        ):
            self.importer.process_data_file(data_file_path, data_file_extension_to_dict)
        with patch.object(
            self.importer, "_file_info", return_value=mock_file_info
        ), patch.object(self.importer, "_read_csv", return_value=participants):
            self.importer.process_metadata_file(
                metadata_file_path, metadata_file_extension_to_dict
            )

        data_to_json = self.importer.processed_data_to_json()

        assert (
            data_to_json[0]
            == '{"metadata": {"path": "study-participant-assessment-day1to4.csv", "filetype": "text/csv", "encoding": "utf-8", "basename": "study-participant-assessment-day1to4.csv", "dirname": "/path/to/files", "mtime": 1234567890.0, "size": 1024, "uid": 1000, "gid": 1000, "mode": 420, "role": "data", "study": "study", "participant": "participant", "assessment": "assessment", "units": "day", "start": "1", "end": "4", "extension": ".csv", "time_end": "4"}, "participant_assessments": [{"var1": 1, "var2": 2, "var3": "str"}, {"var1": 2, "var2": 2, "var3": "str", "var4": 5, "var6": 6, "var7": "str2"}], "assessment_variables": [{"name": "var1", "assessment": "assessment"}, {"name": "var2", "assessment": "assessment"}, {"name": "var3", "assessment": "assessment"}, {"name": "var4", "assessment": "assessment"}, {"name": "var6", "assessment": "assessment"}, {"name": "var7", "assessment": "assessment"}]}'
        )
        assert (
            data_to_json[1]
            == '{"metadata": {"filetype": "text/csv", "encoding": "utf-8", "dirname": "/path/to/files", "mtime": 1234567890.0, "size": 1024, "uid": 1000, "gid": 1000, "mode": 420, "role": "metadata", "study": "site", "extension": ".csv"}, "participants": [{"Active": 1, "Consent": "2022-06-02", "participant": "YA1", "study": "YA"}, {"Active": 1, "Consent": "2022-06-02", "participant": "YA2", "study": "YA"}]}'
        )

        self.importer.data_file["participant_assessments"] = []

        data_to_json = self.importer.processed_data_to_json()

        assert data_to_json == (
            None,
            '{"metadata": {"filetype": "text/csv", "encoding": "utf-8", "dirname": "/path/to/files", "mtime": 1234567890.0, "size": 1024, "uid": 1000, "gid": 1000, "mode": 420, "role": "metadata", "study": "site", "extension": ".csv"}, "participants": [{"Active": 1, "Consent": "2022-06-02", "participant": "YA1", "study": "YA"}, {"Active": 1, "Consent": "2022-06-02", "participant": "YA2", "study": "YA"}]}',
        )
