import csv
import logging

logger = logging.getLogger(__name__)


class CSVDataService:
    def __init__(self, path, max_columns, max_days):
        self.Data = {}
        self.max_columns = max_columns
        self.max_days = max_days
        self.columns = []
        self.day_data = []
        self.path = path

    def _generate_columns(self):
        var_prefix = "var_random_"

        for x in range(0, self.max_columns):
            if x == 2:
                self.columns.append("day")
            self.columns.append(var_prefix + "%d" % (x))

    def _generate_column_data(self):
        for x in range(0, self.max_columns):
            if x == 2:
                self.day_data.append(x)
            if x % 2 == 0 and not x == 2:
                self.day_data.append("value" + " %d" % (x))
            elif x % 1 == 0:
                self.day_data.append(x)

    def prepare_csv_test_data(self):
        self._generate_columns()
        self._generate_column_data()

    def write_test_csv_files(self, participant_num):
        # Write file name to automatically be day1 to maxday

        with open(self.build_file_path(participant_num), "w", newline="") as csvfile:
            csv_writer = csv.writer(
                csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
            )

            csv_writer.writerow(self.columns)

            for x in range(1, self.max_days):
                self.day_data[2] = x
                csv_writer.writerow(self.day_data)

        print(f"Finished writing file {participant_num}")

    def build_file_path(self, participant):
        return (
            self.path
            + "/"
            + f"gnar-eng{participant}-superassessment-day1to"
            + "%d" % (self.max_days)
            + ".csv"
        )
