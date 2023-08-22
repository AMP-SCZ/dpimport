import os
import logging
import mimetypes as mt
from . import patterns
from datetime import datetime
from urllib.parse import quote

from tools import reader

logger = logging.getLogger(__name__)


UNKNOWN = "unknown"
METADATA = "metadata"
DATAFILE = "data"


def probe(path, updated_participants):
    """
    Check file for DPdash compatibility and return a file
    information object.

    :param path: File path
    :type path: str
    """
    if not os.path.exists(path):
        logger.debug("file not found %s", path)
        return None
    dirname = os.path.dirname(path)
    basename = os.path.basename(path)

    # match file and get re match object and file role
    role, match = match_file(basename)
    if role == UNKNOWN:
        return None
    # initialize info object
    info = match.groupdict()
    info["glob"] = path
    if role == DATAFILE:
        info.update(init_datafile(info, updated_participants))
        info["glob"] = get_glob(path)
    # add other necessary information to info object
    mimetype, encoding = mt.guess_type(path)
    stat = os.stat(path)
    info.update(
        {
            "path": path,
            "filetype": mimetype,
            "encoding": encoding,
            "basename": basename,
            "dirname": dirname,
            "dirty": True,
            "synced": False,
            "toc_mtime": stat.st_mtime,
            "size": stat.st_size,
            "uid": stat.st_uid,
            "gid": stat.st_gid,
            "mode": stat.st_mode,
            "role": role,
        }
    )
    return info


def match_file(f):
    match = patterns.DATAFILE.match(f)
    if match:
        return DATAFILE, match
    match = patterns.METADATA.match(f)
    if match:
        return METADATA, match
    return UNKNOWN, None


def init_datafile(info, updated_participants):
    subject = info["subject"]
    assessment = info["assessment"]

    updated_participants.append(
        {
            "subject": subject,
            "assessment": assessment,
        }
    )

    return {
        "subject": subject,
        "assessment": assessment,
        "time_units": str(info["units"]),
        "time_start": int(info["start"]),
        "time_end": int(info["end"]),
    }


def get_glob(f):
    basename = os.path.basename(f)
    dirname = os.path.dirname(f)
    glob = patterns.GLOB_SUB.sub("\\1*\\2", basename)
    return os.path.join(dirname, glob)


def import_file(db, file_info):
    if file_info["role"] == DATAFILE:
        inserted = insert_data(db, file_info)
        if inserted == 0:
            logger.info("Import success for {FILE}".format(FILE=file_info["path"]))
    elif file_info["role"] == METADATA:
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
                if file_info["role"] != METADATA:
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


def sanitize_columns(columns):
    new_columns = []
    for column in columns:
        new_column = quote(str(column).encode("utf-8"), safe="~()*!.'").replace(
            ".", "%2E"
        )
        new_columns.append(new_column)

    return new_columns
