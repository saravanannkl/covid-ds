import sys
import os
import json
import logging
import requests

URL_FORMAT = "https://api.cowin.gov.in/api/v1/reports/v2/getPublicReports?date={}&state_id={}&district_id={}"

SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))

COWIN_DATA_FOLDER_PATH = os.path.join(SCRIPT_FOLDER, "..", "data", "cowin")

TEST_MODE_STATE_IDS = set([31])
TEST_MODE_DISTRICT_IDS = set([571])


def build_url(date, state_id="", district_id=""):
    return URL_FORMAT.format(date, state_id, district_id)

def fetch_data(date, location_type, location_id, url, output_folder):
    file_name = "national" if location_id == 0 else location_id
    file_path = os.path.join(output_folder, "{}.json".format(file_name))
    if os.path.exists(file_path):
        logging.warning("Data file '%s' already has data." % (file_path,))
        return
    data = requests.get(url).json()
    data["date"] = date
    data["location_type"] = location_type
    data["location_id"] = location_id
    with open(file_path, 'w') as file:
        file.write(json.dumps(data))

def extract_national_data(date, output_folder):
    """
        Fetches data aggregated at national level

        Parameters:
            date: Date for which the data needs to be retrieved
    """
    logging.info("Fetching country level data")
    fetch_data(date, "national", 0, build_url(date), output_folder)

def extract_state_data(date, folder_path, test_mode):
    """
        Fetches data aggregated at state level for all states

        Parameters:
            date: Date for which the data needs to be retrieved
            folder_path: Output folder path of the state data
            test_mode: Test mode to test the data extraction.
                Retreives only the whitlisted states in TEST_MODE_STATE_IDS
    """
    logging.info("Fetching state level data")
    with open(os.path.join(SCRIPT_FOLDER, "..", "states.json")) as state_file:
        states = json.load(state_file)
    states_path = os.path.join(folder_path, "states")
    if not os.path.exists(states_path):
        os.makedirs(states_path)
    for state in states:
        state_id = state["id"]
        state_name = state["name"]
        if test_mode and state_id not in TEST_MODE_STATE_IDS:
            logging.warning(
                "Skipping State {}-{}".format(state_id, state_name))
            continue
        logging.info("Processing State: {}".format(state_name))
        url = build_url(date, state_id)
        fetch_data(date, "state", state_id, url, states_path)

def extract_district_data(date, folder_path, test_mode):
    """
        Fetches data aggregated at district level for all districts

        Parameters:
            date: Date for which the data needs to be retrieved
            folder_path: Output folder path of the district data
            test_mode: Test mode to test the data extraction.
                Retreives only the whitlisted states in TEST_MODE_STATE_IDS
    """
    logging.info("Fetching state level data")
    with open(os.path.join(SCRIPT_FOLDER, "..", "districts.json")) as district_file:
        districts = json.load(district_file)
    districts_path = os.path.join(folder_path, "districts")
    if not os.path.exists(districts_path):
        os.makedirs(districts_path)
    for district in districts:
        state_id = district["state_id"]
        district_id = district["district_id"]
        district_name = district["district_name"]
        if test_mode and district_id not in TEST_MODE_DISTRICT_IDS:
            logging.warning(
                "Skipping State {}-{}".format(district_id, district_name))
            continue
        logging.info("Processing State: {}".format(district_name))
        url = build_url(date, state_id, district_id)
        fetch_data(date, "district", district_id, url,districts_path)
    


def extract_data(date, test_mode):
    folder_path = os.path.join(COWIN_DATA_FOLDER_PATH, date)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    extract_national_data(date, folder_path)
    extract_state_data(date, folder_path, test_mode)
    extract_district_data(date, folder_path, test_mode)


if __name__ == "__main__":
    args = sys.argv
    logging.getLogger().setLevel(logging.INFO)
    if(len(args) >= 2):
        date = args[1]
        prod_mode = args[2] if len(args) >= 3 else ""
        extract_data(date, prod_mode != "True")
    else:
        logging.error("Date argument is missing")
