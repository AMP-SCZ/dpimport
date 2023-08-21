#!/usr/bin/env python

import os
import sys
import ssl
import glob
import dppylib
import dpimport
import logging
import argparse as ap
import collections as col
from dpimport.database import Database
from pymongo import DeleteMany, UpdateMany
from pymongo.errors import BulkWriteError

logger = logging.getLogger(__name__)


def main():
    # This takes the yaml and reads it and creates variables
    parser = ap.ArgumentParser()
    parser.add_argument("-d", "--dbname", default="dpdmongo")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("expr")
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(level=level)

    # connect to database
    db = Database(args.dbname).connect()
    updated_participants = []
    # iterate over matching files on the filesystem
    for f in glob.iglob(args.expr, recursive=True):
        basename = os.path.basename(f)
        # probe for dpdash-compatibility and gather information
        probe = dpimport.probe(f, updated_participants)
        if not probe:
            logger.debug("document is unknown %s", basename)
            continue
        # nothing to be done
        # import the file
        logger.info("importing file %s", f)
        dppylib.import_file(db.db, probe)

    update_last_day(db.db, updated_participants)


def update_last_day(db, list_of_updated_participants):
    try:
        for participant in list_of_updated_participants:
            query = {
                "subject": participant["subject"],
                "assessment": participant["assessment"],
            }
            last_day_cursor = db.assessmentSubjectDayData.aggregate(
                [{"$match": query}, {"$group": {"_id": None, "end": {"$max": "$end"}}}]
            )
            cursor_data = next(last_day_cursor, None)
            participant.update({"end": cursor_data["end"]})

        for participant in list_of_updated_participants:
            db.assessmentSubjectDayData.update_many(
                query,
                {"$set": {"end": participant["end"], "time_end": participant["end"]}},
            )

    except Exception as e:
        logger.error(e)
        return 1


if __name__ == "__main__":
    main()
