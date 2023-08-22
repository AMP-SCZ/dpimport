#!/usr/bin/env python

import os
import sys
import ssl
import glob
import dpimport
import logging
import argparse as ap
import collections as col
from dpimport.database import Database
from pymongo import DeleteMany, UpdateMany
from pymongo.errors import BulkWriteError

logger = logging.getLogger(__name__)


def main():
    parser = ap.ArgumentParser()
    parser.add_argument("-d", "--dbname", default="dpdmongo")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("expr")
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(level=level)

    db = Database(args.dbname).connect()
    updated_participants = []

    # iterate over matching files on the filesystem
    for f in glob.iglob(args.expr, recursive=True):
        dirname = os.path.dirname(f)
        basename = os.path.basename(f)
        # probe for dpdash-compatibility and gather information
        probe = dpimport.probe(f, updated_participants)
        if not probe:
            logger.debug("document is unknown %s", basename)
            continue
        # import the file
        logger.info("importing file %s", f)
        dpimport.import_file(db.db, probe)

    logger.info("cleaning metadata")
    update_last_day(db.db, updated_participants)


def update_last_day(db, list_of_updated_participants):
    try:
        for participant in list_of_updated_participants:
            last_day_cursor = db.assessmentSubjectDayData.aggregate(
                [
                    {"$match": participant},
                    {"$group": {"_id": None, "end": {"$max": "$end"}}},
                ]
            )
            cursor_data = next(last_day_cursor, None)
            participant.update({"end": cursor_data["end"]})

        for participant in list_of_updated_participants:
            query = {
                "subject": participant["subject"],
                "assessment": participant["assessment"],
            }

            db.assessmentSubjectDayData.update_many(
                query,
                {"$set": {"end": participant["end"], "time_end": participant["end"]}},
            )

    except Exception as e:
        logger.error(e)
        return 1


if __name__ == "__main__":
    main()
