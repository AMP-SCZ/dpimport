import os
import re
import logging
import mimetypes as mt
from datetime import datetime
from urllib.parse import quote

from tools import reader

TIME_UNITS = {"day": "days", "hr": "hours"}

_UNITS = "|".join(TIME_UNITS.keys())
_EXTENSION = ".csv"

FILE_REGEX = re.compile(
    r"(?P<study>\w+)\-(?P<subject>\w+)\-(?P<assessment>\w+)\-(?P<units>{UNITS})(?P<start>[+-]?\d+(?:\.\d+)?)to(?P<end>[+-]?\d+(?:\.\d+)?)(?P<extension>{EXTENSION})".format(
        UNITS=_UNITS, EXTENSION=_EXTENSION
    )
)
FILE_SUB = re.compile(
    r"(\w+\-\w+\-\w+\-{UNITS})[+-]?\d+(?:\.\d+)?to[+-]?\d+(?:\.\d+)?(.*)".format(
        UNITS=_UNITS
    )
)
METADATA_REGEX = re.compile(
    r"(?P<study>\w+)\_metadata(?P<extension>{EXTENSION})".format(EXTENSION=".csv")
)

logger = logging.getLogger(__name__)


# Verify if a file is DPdash-compatible file, and return file info if so.
def stat_file(import_dir, file_name, file_path):
    file_info = match_file(file_name, import_dir)
    if not file_info:
        return None

    filetype, encoding = guess_type(file_info["extension"])
    if not os.path.exists(file_path):
        return None

    file_stat = os.stat(file_path)
    file_info.update(
        {
            "path": file_path,
            "filetype": filetype,
            "encoding": encoding,
            "basename": file_name,
            "dirname": import_dir,
            "synced": True,
            "dirty": True,
            "mtime": file_stat.st_mtime,
            "size": file_stat.st_size,
            "uid": file_stat.st_uid,
            "gid": file_stat.st_gid,
            "mode": file_stat.st_mode,
        }
    )

    return file_info


def import_file(db, file_info):
    if file_info["role"] == "data":
        inserted = insert_data(db, file_info)
        if inserted == 0:
            logger.info("Import success for {FILE}".format(FILE=file_info["path"]))
    elif file_info["role"] == "metadata":
        collection = db["metadata"]

        ref_id = insert_reference(collection, file_info)
        if ref_id is None:
            logger.error("Unable to import {FILE}".format(FILE=file_info["path"]))
            return
    else:
        logger.error(
            "{FILE} is not compatible with DPdash. Exiting import.".format(
                FILE=file_info["path"]
            )
        )
        return


# Insert the reference doc, returns the inserted id
def insert_reference(collection, reference):
    try:
        ref_id = collection.insert_one(reference).inserted_id
        return ref_id
    except Exception as e:
        logger.error(e)
        return None


# Insert the data
def insert_data(db, file_info):
    try:
        day_data_collection = db.assessmentSubjectDayData
        query = {
            "study": file_info["study"],
            "assessment": file_info["assessment"],
            "subject": file_info["subject"],
        }
        participant_data = prepare_data(day_data_collection, file_info, query)

        if len(participant_data["new_data"]) > 0:
            logger.info("Importing new data...")
            day_data_collection.insert_many(participant_data["new_data"], False)

        if len(participant_data["updated_data"]) > 0:
            for updated_day_data in participant_data["updated_data"]:
                query["day"] = updated_day_data["day"]

                updated_day_data["synced"]: True
                updated_day_data["updated"]: datetime.utcnow()

                logger.info("Updating data... ")
                day_data_collection.update_one(query, {"$set": updated_day_data})

        return 0

    except Exception as e:
        logger.error(e)
        return 1


def prepare_data(day_data_collection, file_info, query):
    try:
        participant_data = {"new_data": [], "updated_data": []}
        for chunk in reader.read_csv(file_info["path"]):
            if len(chunk) > 0:
                if file_info["role"] != "metadata":
                    chunk_columns = sanitize_columns(chunk.columns.values.tolist())
                    chunk.columns = chunk_columns
                new_data = chunk.to_dict("records")
                for day_data in new_data:
                    query["day"] = day_data["day"]
                    current_document = day_data_collection.find_one(query)

                    if current_document == None:
                        participant_data["new_data"].append({**day_data, **file_info})

                    elif current_document["mtime"] == day_data["mtime"]:
                        logger.info("Data has already been updated, continuing")

                        continue
                    elif current_document["mtime"] != day_data["mtime"]:
                        file_path = file_info["path"]

                        logger.info(
                            "Data has been updated, adding to list. {FILE}".format(
                                FILE=file_path
                            )
                        )
                        participant_data["updated_data"].append(
                            {**day_data, **file_info}
                        )

        return participant_data

    except Exception as e:
        logger.error(e)
        return 1


# Rename columns to encode special characters
def sanitize_columns(columns):
    new_columns = []
    for column in columns:
        new_column = quote(str(column).encode("utf-8"), safe="~()*!.'").replace(
            ".", "%2E"
        )
        new_columns.append(new_column)

    return new_columns


# Match the filename to distinguish data from metadata files
def match_file(file_name, sub_dir):
    matched_file = FILE_REGEX.match(file_name)
    if not matched_file:
        logger.info("file did not match %s", file_name)
        matched_metadata = METADATA_REGEX.match(file_name)
        if not matched_metadata:
            return None
        else:
            return scan_metadata(matched_metadata, file_name, sub_dir)
    else:
        logger.info("file matched %s", file_name)
        return scan_data(matched_file, file_name, sub_dir)


# Return file_info for the metadata
def scan_metadata(match, file_name, sub_dir):
    file_info = match.groupdict()

    file_info.update({"glob": os.path.join(sub_dir, file_name), "role": "metadata"})

    return file_info


# Return file_info for the data
def scan_data(match, file_name, sub_dir):
    file_info = match.groupdict()

    file_info.update(
        {
            "subject": file_info["subject"],
            "assessment": file_info["assessment"],
            "glob": os.path.join(sub_dir, FILE_SUB.sub("\\1*\\2", file_name)),
            "time_units": str(file_info["units"]),
            "time_start": int(file_info["start"]),
            "time_end": int(file_info["end"]),
            "role": "data",
        }
    )

    return file_info


# get mime type and encoding
def guess_type(extension):
    return mt.guess_type("file{}".format(extension))


class StatError(Exception):
    pass


class ParserError(Exception):
    pass
