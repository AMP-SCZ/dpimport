#!/usr/bin/env python

import os
import yaml
import argparse as ap
from importer_service import api_importer_service


def main():
    parser = ap.ArgumentParser()
    parser.add_argument("-c", "--config")
    args = parser.parse_args()

    with open(os.path.expanduser(args.config), "r") as fo:
        config = yaml.load(fo, Loader=yaml.SafeLoader)

    api_url = config["api_url"]
    credentials = {
        "x-api-user": config["api_user"],
        "x-api-key": config["api_key"],
    }
    api = api_importer_service.ImporterApiService(credentials)
    print(api.routes(api_url, "metadata"), "THE ROUTE")
    api.refresh_metadata_collection(api.routes(api_url, "metadata"))


if __name__ == "__main__":
    main()
