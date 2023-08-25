import os
import yaml
import argparse as ap
from csv_data_service import csv_data_service
from concurrent.futures import ProcessPoolExecutor


def main():
    parser = ap.ArgumentParser()
    parser.add_argument("-c", "--config")
    args = parser.parse_args()

    with open(os.path.expanduser(args.config), "r") as fo:
        config = yaml.load(fo, Loader=yaml.SafeLoader)

    dir_path = config["csv_directory"]
    data_columns = config["columns"]
    range_of_days = config["max_days"]
    num_of_participants = config["participant_count"]

    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)

    csv_service = csv_data_service.CSVDataService(dir_path, data_columns, range_of_days)
    csv_service.prepare_csv_test_data()

    with ProcessPoolExecutor(4) as exe:
        exe.map(csv_service.write_test_csv_files, range(num_of_participants))

    print("Finished Writing files")


if __name__ == "__main__":
    main()
