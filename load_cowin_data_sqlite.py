import os
import io
import logging
import json
import csv
from sqlite_utils import Database

# CSV File and table mapping
TABLE_FILE_MAPPING = {
    "vaccination_site_count.csv": {
        "table": "vaccination_site_count",
        "load_location_columns": True,
        "pk": ("location_type", "location_id", "date")
    },
    "vaccination_session_count.csv": {
        "table": "vaccination_session_count",
        "load_location_columns": True,
        "pk": ("location_type", "location_id", "date")
    },
    "meta.csv": {
        "table": "meta",
        "load_location_columns": True,
        "pk": (
            "location_type",
            "location_id",
            "date"
        )
    },
    "vaccination_session_count.csv": {
        "table": "vaccination_session_count",
        "load_location_columns": True,
        "pk": (
            "location_type",
            "location_id",
            "date"
        )
    },
    "registration_count.csv": {
        "table": "registration_count",
        "load_location_columns": True,
        "pk": (
            "location_type",
            "location_id",
            "date"
        )
    },
    "vaccination_by_age.csv": {
        "table": "vaccination_by_age",
        "load_location_columns": True,
        "pk": (
            "location_type",
            "location_id",
            "date"
        )
    },
    "vaccination_count.csv": {
        "table": "vaccination_count",
        "load_location_columns": True,
        "pk": (
            "location_type",
            "location_id",
            "date"
        )
    },
    "session_vaccination_count.csv": {
        "table": "session_vaccination_count",
        "load_location_columns": True,
        "pk": (
            "location_type",
            "location_id",
            "date",
            "label"
        )
    },
    "site_level_vaccination_count.csv": {
        "table": "site_level_vaccination_count",
        "load_location_columns": True,
        "pk": (
            "location_type",
            "location_id",
            "date",
            "session_site_id"
        )
    },
    "state_level_vaccination_count.csv": {
        "table": "state_level_vaccination_count",
        "load_location_columns": False,
        "pk": (
            "date",
            "state_id"
        )
    },
    "last_7days_vaccination_count.csv": {
        "table": "daily_vaccination_count_snapshot",
        "load_location_columns": True,
        "pk": (
            "location_type",
            "location_id",
            "date",
            "vaccine_date"
        )
    },
    "last_7days_registration_count.csv": {
        "table": "daily_registration_count_snapshot",
        "load_location_columns": True,
        "pk": (
            "location_type",
            "location_id",
            "date",
            "reg_date"
        )
    },
    "last_5days_session_status.csv": {
        "table": "daily_session_status_snapshot",
        "load_location_columns": True,
        "pk": (
            "location_type",
            "location_id",
            "date",
            "session_date"
        )
    },
    "district_level_vaccination_count.csv": {
        "table": "district_level_vaccination_count",
        "load_location_columns": False,
        "pk": (
            "date",
            "district_id"
        )
    }
}


def get_json_from_file(path):
    """
    Load JSON file as Python JSON object.

    Parameters:
        Full path of the JSON file

    Returns:
        Python JSON representation of the JSON in the file
    """
    with open(path) as file:
        return json.load(file)


def load_csv_file(db, table, csv_file, location_col_values, extra_kwargs):
    """
        Loads CSV file into a table.

        Parameters:
            db: Sqlite database connection
            table: Table name into which the table needs to be loaded.
            csv_file: Path of the CSV file that needs to be loaded.
            location_col_values: If the columns location_type and location_id
                needs to be added to the table, then that values
            extra_kwargs: Additional arguments that needs to be passed
                to insert statement
    """
    with open(csv_file) as file:
        reader = csv.reader(file)
        headers = next(reader)
        if len(location_col_values) > 0:
            headers = ["location_type", "location_id"] + headers
        records = (dict(zip(headers, location_col_values + row))
                   for row in reader)
        db[table].insert_all(records, **extra_kwargs)


def load_state_district_meta_data(db):
    """
        Loads State and District data from JSON files.

        Parameters:
            db: Sqlite db connection
    """
    db["states"].insert_all(get_json_from_file("states.json"), pk="id")
    db["districts"].insert_all(get_json_from_file(
        "districts.json"), pk="district_id", foreign_keys=[("state_id", "states")])
    db["districts"].transform(
        rename={"district_id": "id", "district_name": "name"})


def load_location_data_files(db, folder, location_type, location_id, date):
    """
        Loads location data csv files in a folder into database.

        Parameters:
            db: Sqlite database connection
            folder: Folder that contains CSV files
            location_type: Type of the location for which the data
                is loaded. E.g. national, state, district
            location_id: ID of the location. ID for national will be 0.
            date: fdate for which the data is loaded.
    """
    for file in os.listdir(folder):
        file_path = os.path.join(folder, file)
        if not os.path.isfile(file_path):
            logging.warning("Skipping invalid file {}".format(file))
            continue

        table_load_info = TABLE_FILE_MAPPING.get(file)
        if not table_load_info:
            logging.warning("No mapping available for file {}".format(file))
            continue

        logging.info("Processing file {}".format(file))
        table = table_load_info.get("table")
        pk = table_load_info.get("pk")
        extra_kwargs = {"batch_size": 100}

        if pk:
            extra_kwargs["pk"] = pk

        location_col_values = []
        if table_load_info.get("load_location_columns"):
            location_col_values = [location_type, location_id]

        load_csv_file(db, table, os.path.join(folder, file),
                      location_col_values, extra_kwargs)
