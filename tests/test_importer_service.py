from unittest import TestCase
from unittest.mock import patch
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

        assert self.importer.diverge_files(path) == None

        with patch.object(
            self.importer, "process_data_file"
        ) as mocked_process_data_file:
            data_file_path = "study-subject-assessment-day1to4.csv"

            self.importer.diverge_files(data_file_path)
            mocked_process_data_file.assert_called_once()

        with patch.object(
            self.importer, "process_metadata_file"
        ) as mocked_process_metadata_file:
            metadata_file_path = "site_metadata.csv"

            self.importer.diverge_files(metadata_file_path)
            mocked_process_metadata_file.assert_called_once()

    def test_processed_data_to_json(self):
        data_file_path = "study-subject-assessment-day1to4.csv"
        data_file_extension_to_dict = self.importer.DATAFILE.match(
            data_file_path
        ).groupdict()
        mock_data_file_info = {
            "path": "study-subject-assessment-day1to4.csv",
            "filetype": "text/csv",
            "encoding": "utf-8",
            "basename": "study-subject-assessment-day1to4.csv",
            "dirname": "/path/to/files",
            "mtime": 1234567890.0,
            "size": 1024,
            "uid": 1000,
            "gid": 1000,
            "mode": 0o644,
        }
        subject_assessments = [
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
            {"Subject ID": "YA1", "Active": 1, "Consent": "-", "Study": "YA"},
            {"Subject ID": "YA2", "Active": 1, "Consent": "-", "Study": "YA"},
        ]

        with patch.object(
            self.importer, "_file_info", return_value=mock_data_file_info
        ), patch.object(
            self.importer, "_read_csv", return_value={"columns": []}
        ), patch.object(
            self.importer, "collect_csv_row", return_value=subject_assessments
        ):
            self.importer.process_data_file(data_file_path, data_file_extension_to_dict)
        with patch.object(
            self.importer, "_file_info", return_value=mock_file_info
        ), patch.object(
            self.importer, "_read_csv", return_value={"columns": []}
        ), patch.object(
            self.importer, "collect_csv_row", return_value=participants
        ):
            self.importer.process_metadata_file(
                metadata_file_path, metadata_file_extension_to_dict
            )

        data_to_json = self.importer.processed_data_to_json()

        assert (
            data_to_json[0]
            == '{"metadata": {"path": "study-subject-assessment-day1to4.csv", "filetype": '
            '"text/csv", "encoding": "utf-8", "basename": '
            '"study-subject-assessment-day1to4.csv", "dirname": "/path/to/files", '
            '"mtime": 1234567890.0, "size": 1024, "uid": 1000, "gid": 1000, "mode": 420, '
            '"role": "data", "collection": '
            '"5e74265e4a4d3760737bbf39a513a938b5bd333a958c699d5006ae026ae0017a", "study": '
            '"study", "subject": "subject", "assessment": "assessment", "units": "day", '
            '"start": "1", "end": "4", "extension": ".csv", "time_end": "4"}, '
            '"subject_assessments": [{"var1": 1, "var2": 2, "var3": "str"}, {"var1": 2, '
            '"var2": 2, "var3": "str", "var4": 5, "var6": 6, "var7": "str2"}]}'
        )
        assert (
            data_to_json[1]
            == '{"metadata": {"filetype": "text/csv", "encoding": "utf-8", "dirname": "/path/to/files", "mtime": 1234567890.0, "size": 1024, "uid": 1000, "gid": 1000, "mode": 420, "role": "metadata", "study": "site", "extension": ".csv"}, "participants": [{"Subject ID": "YA1", "Active": 1, "Consent": "-", "Study": "YA"}, {"Subject ID": "YA2", "Active": 1, "Consent": "-", "Study": "YA"}]}'
        )

        self.importer.data_file["subject_assessments"] = []

        data_to_json = self.importer.processed_data_to_json()

        assert data_to_json == (
            None,
            '{"metadata": {"filetype": "text/csv", "encoding": "utf-8", "dirname": "/path/to/files", "mtime": 1234567890.0, "size": 1024, "uid": 1000, "gid": 1000, "mode": 420, "role": "metadata", "study": "site", "extension": ".csv"}, "participants": [{"Subject ID": "YA1", "Active": 1, "Consent": "-", "Study": "YA"}, {"Subject ID": "YA2", "Active": 1, "Consent": "-", "Study": "YA"}]}',
        )
