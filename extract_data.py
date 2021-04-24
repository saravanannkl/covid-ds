import sys
import os
import json
import logging
import requests

URL_FORMAT = "https://api.cowin.gov.in/api/v1/reports/v2/getPublicReports?date={}&state_id={}&district_id={}"

def build_url(date, state_id="", district_id=""):
    return URL_FORMAT.format(date, state_id, district_id)

def fetch_data(url, result_path):
    if os.path.exists(result_path):
        logging.warning("File '%s' already exists." % (result_path,))
        return
    data = requests.get(url).json()
    with open(result_path, 'w') as out_file:
        json.dump(data, out_file)

def get_state_districts_data():
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
    logging.info("Fetching country level data")
    url = build_url(date)
    file_name = "cowin-national-{}.json".format(date);
    fetch_data(url, os.path.join("data", file_name))

def extract_state_data(state_district_data, date):
    logging.info("Fetching state level data")
    for state_id, state_info in state_district_data.items():
        state_name = state_info["name"]
        logging.info("Processing State: {}".format(state_name))
        url = build_url(date, state_id)
        fmt_state_name = state_name.replace(" and", "").replace(" ", "")
        folder = os.path.join("data", "{}-{}".format(state_id, fmt_state_name))
        if not os.path.exists(folder):
            os.makedirs(folder)
        file_name = "cowin-state-{}-{}.json".format(fmt_state_name, date)
        fetch_data(url, os.path.join(folder, file_name))

def extract_district_data(state_district_data, date):
    logging.info("Fetching district level data")
    for state_id, state_info in state_district_data.items():
        state_name = state_info["name"]
        fmt_state_name = state_name.replace(" and", "").replace(" ", "")
        state_folder = os.path.join("data", "{}-{}".format(state_id, fmt_state_name))
        if not os.path.exists(state_folder):
            os.makedirs(state_folder)
        for district in state_info["districts"]:
            district_id = district["id"]
            district_name = district["name"]
            logging.info("Processing District: {}, {}".format(district_name, state_name))
            url = build_url(date, state_id, district_id)
            fmt_dist_name = district_name.replace("and", "").replace(" ", "")
            folder = os.path.join(state_folder, "{}-{}".format(district_id, fmt_dist_name))
            file_name = "cowin-district-{}-{}.json".format(fmt_dist_name, date)
            if not os.path.exists(folder):
                os.makedirs(folder)
            fetch_data(url, os.path.join(folder, file_name))


def extract_data(date):
    logging.info("Extracting data for '{}'".format(date))
    state_district_data = get_state_districts_data()
    extract_national_data(date)
    extract_state_data(state_district_data, date)
    extract_district_data(state_district_data, date)

if __name__ == "__main__":
    if(len(sys.argv) >= 2):
        date = sys.argv[1]
        extract_data(date)
    else:
        logging.error("Date argument is missing")
