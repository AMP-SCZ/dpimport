import re
import os
import hashlib
import json
import polars as pl
from urllib.parse import quote
import mimetypes as mt
import logging

logger = logging.getLogger(__name__)


class DataImporterService:
    DATAFILE = re.compile(
        r"(?P<study>\w+)\-(?P<subject>\w+)\-(?P<assessment>\w+)\-(?P<units>day)(?P<start>[+-]?\d+(?:\.\d+)?)to(?P<end>[+-]?\d+(?:\.\d+)?)(?P<extension>.csv)"
    )
    METADATA = re.compile(r"(?P<study>\w+)\_metadata(?P<extension>.csv)")
    GLOB_SUB = re.compile(
        r"(\w+\-\w+\-\w+\-day)[+-]?\d+(?:\.\d+)?to[+-]?\d+(?:\.\d+)?(.*)"
    )
    records = "records"
    metadata_key = "metadata"

    def __init__(self, data_file, metadata_file):
        self.data_file = data_file
        self.metadata_file = metadata_file

    def process_file(self, path):
        basename = os.path.basename(path)
        is_data_file = self.DATAFILE.match(basename)
        is_metadata_file = self.METADATA.match(basename)

        if is_data_file:
            file_extension_to_dict = self.DATAFILE.match(basename).groupdict()
            return self.process_data_file(path, file_extension_to_dict)
        if is_metadata_file:
            metadata_file_extension = self.METADATA.match(basename).groupdict()

            return self.process_metadata_file(path, metadata_file_extension)
        else:
            return None

    def process_data_file(self, path, file_extension):
        file_extension.update({"time_end": file_extension["end"]})
        collection_base = "{study}{subject}{assessment}".format(
            study=file_extension["study"],
            subject=file_extension["subject"],
            assessment=file_extension["assessment"],
        ).encode("utf-8")
        hash_collection = hashlib.sha256(collection_base).hexdigest()
        metadata = self._file_info(path)
        metadata.update(
            {"role": "data", "collection": hash_collection, **file_extension}
        )
        del file_extension["extension"]

        subject_assessments = self._read_csv(
            path,
        )
        self.data_file.update(
            {"metadata": metadata, "subject_assessments": subject_assessments}
        )
        return

    def process_metadata_file(self, path, file_extension):
        metadata = self._file_info(path)
        participants = self._read_csv(path)

        metadata.update(
            {
                "role": "metadata",
                **file_extension,
            }
        )
        self.metadata_file.update(
            {
                "metadata": metadata,
                "participants": participants,
            }
        )

        return

    def _read_csv(self, file_path):
        df = pl.read_csv(file_path)
        return df.to_dicts()

    def _file_info(self, path):
        mimetype, encoding = mt.guess_type(path)
        dirname = os.path.dirname(path)
        basename = os.path.basename(path)
        stat = os.stat(path)

        return {
            "path": path,
            "filetype": mimetype,
            "encoding": encoding,
            "basename": basename,
            "dirname": dirname,
            "mtime": stat.st_mtime,
            "size": stat.st_size,
            "uid": stat.st_uid,
            "gid": stat.st_gid,
            "mode": stat.st_mode,
        }

    def processed_data_to_json(self):
        processed_data = (
            json.dumps(self.data_file)
            if self.data_file and len(self.data_file["subject_assessments"]) > 0
            else None
        )
        processed_metadata = (
            json.dumps(self.metadata_file)
            if self.metadata_file and len(self.metadata_file["participants"]) > 0
            else None
        )
        print("processed file data to json")
        return processed_data, processed_metadata
