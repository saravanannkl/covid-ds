import sys
import os
import json
import logging
import requests
from data_converter import convert_data

URL_FORMAT = "https://api.cowin.gov.in/api/v1/reports/v2/getPublicReports?date={}&state_id={}&district_id={}"

COWIN_DATA_FOLDER_PATH = os.path.join("data", "cowin")

TEST_MODE_STATE_IDS = set([31])
TEST_MODE_DISTRICT_IDS = set([571])


def build_url(date, state_id="", district_id=""):
    return URL_FORMAT.format(date, state_id, district_id)


def normalize_name(name):
    return name.replace(" and", " And").replace(" ", "")


def fetch_data(date, location_type, url, result_path):
    """
        Fetches data from COWIN Dashboard, converts into CSV files
        and stores them in the result path.

        Parameters:
            date: Date for which data needs to retrieved
            location_type: E.g. national, state, district
            url: COWIN API URL with query params
            result_path: Location where the csv data is stored
    """
    if os.path.exists(result_path):
        logging.warning("Folder '%s' already has data." % (result_path,))
        return
    os.makedirs(result_path)
    data = requests.get(url).json()
    data["date"] = date
    convert_data(location_type, result_path, data)


def get_state_districts_data():
    """
    Loads state and district data in hierarchial format from json data files
    """
    with open("states.json") as state_file:
        states = json.load(state_file)
    data = {}
    for state in states:
        data[state["id"]] = {"name": state["name"], "districts": []}
    with open("districts.json") as district_file:
        districts = json.load(district_file)
    for district in districts:
        state_id = district["state_id"]
        id = district["district_id"]
        name = district["district_name"]
        data[state_id]["districts"].append({"id": id, "name": name})
    return data


def extract_national_data(date):
    """
        Fetches data aggregated at national level

        Parameters:
            date: Date for which the data needs to be retrieved
    """
    logging.info("Fetching country level data")
    url = build_url(date)
    folder_path = os.path.join(COWIN_DATA_FOLDER_PATH, date)
    fetch_data(date, "national", url, folder_path)


def extract_state_data(state_district_data, date, test_mode):
    """
        Fetches data aggregated at state level for all states

        Parameters:
            state_district_data: State data as a dictionary
            date: Date for which the data needs to be retrieved
            test_mode: Test mode to test the data extraction.
                Retreives only the whitlisted states in TEST_MODE_STATE_IDS
    """
    logging.info("Fetching state level data")
    for state_id, state_info in state_district_data.items():
        state_name = state_info["name"]
        if test_mode and state_id not in TEST_MODE_STATE_IDS:
            logging.warning(
                "Skipping State {}-{}".format(state_id, state_name))
            continue
        logging.info("Processing State: {}".format(state_name))
        url = build_url(date, state_id)
        fmt_state_name = normalize_name(state_name)
        folder = os.path.join(COWIN_DATA_FOLDER_PATH,
                              "{}-{}".format(state_id, fmt_state_name))
        if not os.path.exists(folder):
            os.makedirs(folder)
        data_folder_path = os.path.join(folder, date)
        fetch_data(date, "state", url, data_folder_path)


def extract_district_data(state_district_data, date, test_mode):
    """
        Fetches data aggregated at district level for all districts

        Parameters:
            state_district_data: District level data grouped by state
            date: Date for which the data needs to be retrieved
            test_mode: Test mode to test the data extraction.
                Retreives only the whitlisted districts in TEST_MODE_DISTRICT_IDS
    """
    logging.info("Fetching district level data")
    for state_id, state_info in state_district_data.items():
        state_name = state_info["name"]
        fmt_state_name = normalize_name(state_name)
        state_folder = os.path.join(
            COWIN_DATA_FOLDER_PATH, "{}-{}".format(state_id, fmt_state_name))
        if not os.path.exists(state_folder):
            os.makedirs(state_folder)
        for district in state_info["districts"]:
            district_id = district["id"]
            district_name = district["name"]
            if test_mode and district_id not in TEST_MODE_DISTRICT_IDS:
                logging.warning(
                    "Skipping District {}-{}".format(district_id, district_name))
                continue
            logging.info("Processing District: {}, {}".format(
                district_name, state_name))
            url = build_url(date, state_id, district_id)
            fmt_dist_name = normalize_name(district_name)
            folder = os.path.join(
                state_folder, "{}-{}".format(district_id, fmt_dist_name))
            data_folder_path = os.path.join(folder, date)
            if not os.path.exists(folder):
                os.makedirs(folder)
            fetch_data(date, "district", url, data_folder_path)


def extract_data(date, test_mode):
    """
        Extracts national, state and district level data for a date

        Parameters:
            date: Date for which the data needs to be retrieved
            test_mode: Extract only whitelisted entries
    """
    logging.info("Extracting data for '{}'".format(date))
    state_district_data = get_state_districts_data()
    extract_national_data(date)
    extract_state_data(state_district_data, date, test_mode)
    extract_district_data(state_district_data, date, test_mode)


if __name__ == "__main__":
    args = sys.argv
    logging.getLogger().setLevel(logging.INFO)
    if(len(args) >= 2):
        date = args[1]
        prod_mode = args[2] if len(args) >= 3 else ""
        extract_data(date, prod_mode != "True")
    else:
        logging.error("Date argument is missing")
