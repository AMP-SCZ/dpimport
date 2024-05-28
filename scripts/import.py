#!/usr/bin/env python

import os
import glob
import yaml
import argparse as ap
from data_importer_service import data_import_service
from importer_service import api_importer_service


def main():
    parser = ap.ArgumentParser()
    parser.add_argument("-c", "--config")
    parser.add_argument("expr")
    args = parser.parse_args()

    with open(os.path.expanduser(args.config), "r") as fo:
        config = yaml.load(fo, Loader=yaml.SafeLoader)

    api_url = config["api_url"]
    credentials = {
        "x-api-user": config["api_user"],
        "x-api-key": config["api_key"],
    }
    api = api_importer_service.ImporterApiService(credentials)

    for file in glob.iglob(args.expr, recursive=True):
        data_file = {}
        metadata_file = {}
        importer_service = data_import_service.DataImporterService(
            data_file, metadata_file
        )
        importer_service.process_file(file)
        data, meta = importer_service.processed_data_to_json()
        if data:
            print("Day Data struct:")
            print(data)
            api.upsert_file(api.routes(api_url, "day_data"), data)

        if meta:
            print("Metadata struct:")
            print(meta)

            api.upsert_file(api.routes(api_url, "metadata"), meta)


if __name__ == "__main__":
    main()
