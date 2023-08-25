#!/usr/bin/env python

import os
import glob
import yaml
import logging
import argparse as ap
from data_importer_service import data_import_service
from api import api

logger = logging.getLogger(__name__)


def main():
    parser = ap.ArgumentParser()
    parser.add_argument("-c", "--config")
    parser.add_argument("expr")
    args = parser.parse_args()

    with open(os.path.expanduser(args.config), "r") as fo:
        config = yaml.load(fo, Loader=yaml.SafeLoader)

    api_url = config["api_url"]

    # iterate over matching files on the filesystem
    for file in glob.iglob(args.expr, recursive=True):
        data_file = {}
        metadata_file = {}
        importer_service = data_import_service.DataImporterService(
            data_file, metadata_file
        )
        importer_service.process_file(file)
        data, meta = importer_service.processed_data_to_json()

        if data:
            api.upsert_file(api.routes(api_url, "day_data"), data)

        if meta:
            api.upsert_file(api.routes(api_url, "metadata"), meta)


if __name__ == "__main__":
    main()
