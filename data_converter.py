import json
import os
import csv

DATA_MAPPING = {
    "vaccination_site_count": {
        "type": "object",
        "columns": ["total", "govt", "pvt", "today"],
        "path": "topBlock.sites"
    },
    "registration_count": {
        "type": "object",
        "columns": ["total", "male", "female", "others", "online", "onspot", "today", "flwAndHcw"],
        "path": "topBlock.registration"
    },
    "vaccination_session_count": {
        "type": "object",
        "columns": ["total", "govt", "pvt", "today"],
        "path": "topBlock.sessions"

    },
    "vaccination_count": {
        "type": "object",
        "columns": [
            "total",
            "male",
            "female",
            "others",
            "covishield",
            "covaxin",
            "today",
            "tot_dose_1",
            "tot_dose_2",
            "total_doses",
            "aefi"
        ],
        "path": "topBlock.vaccination"
    },
    "vaccination_by_age": {
        "type": "object",
        "columns": ["total", "vac_18_25", "vac_25_40", "vac_40_60", "above_60"],
        "path": "vaccinationByAge"
    },
    "session_vaccination_count": {
        "type": "object_list",
        "columns": ["ts", "timestamps", "label", "count", "dose_one", "dose_two"],
        "path": "vaccinationDoneByTime"
    },
    "last_7days_registration_count": {
        "type": "object_list",
        "columns": ["reg_date", "total", "male", "female", "others"],
        "path": "last7DaysRegistration"
    },
    "last_7days_vaccination_count": {
        "type": "object_list",
        "columns": ["vaccine_date", "count", "dose_one", "dose_two", "covishield", "covaxin", "aefi"],
        "path": "last7DaysVaccination"
    },
    "last_5days_session_status": {
        "type": "object_list",
        "columns": ["session_date", "total", "planned", "ongoing", "completed"],
        "path": "last5daySessionStatus"
    },
    "meta": {
        "type": "object",
        "columns": ["timestamp", "aefiPercentage"],
        "path": "."
    },
    "state_level_vaccination_count": {
        "location_type": "national",
        "type": "object_list",
        "columns": ["state_id", "state_name", "total", "partial_vaccinated", "totally_vaccinated", "today"],
        "path": "getBeneficiariesGroupBy"
    },
    "district_level_vaccination_count": {
        "location_type": "state",
        "type": "object_list",
        "columns": ["state_id", "district_id", "district_name", "total", "partial_vaccinated", "totally_vaccinated", "today"],
        "path": "getBeneficiariesGroupBy"
    },
    "site_level_vaccination_count": {
        "location_type": "district",
        "type": "object_list",
        "columns": ["session_site_id", "title", "session_site_name", "total", "partial_vaccinated", "totally_vaccinated", "today"],
        "path": "getBeneficiariesGroupBy"
    }
}


def get_nested_object(obj, path_key):
    if path_key == ".":
        return obj
    parts = path_key.split(".")
    value = obj
    for part in parts:
        value = value.get(part)
        if not value:
            break
    return value


def convert_object_to_csv(obj, date, columns, file_path):
    with open(file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(["date", *columns])
        data = map(lambda column: obj.get(column), columns)
        csv_writer.writerow([date, *data])


def convert_object_array_to_csv(obj_list, date, columns, file_path):
    with open(file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(["date", *columns])
        for obj in obj_list:
            data = map(lambda column: obj.get(column), columns)
            csv_writer.writerow([date, *data])


def convert_data(location_type, folder, data):
    for data_type, config in DATA_MAPPING.items():
        c_location_type = config.get("location_type")
        if c_location_type and c_location_type != location_type:
            continue
        mapping_type = config["type"]
        obj = get_nested_object(data, config["path"])
        file_path = os.path.join(folder, "{}.csv".format(data_type))
        if mapping_type == "object":
            convert_object_to_csv(
                obj, data["date"], config["columns"], file_path)
        elif mapping_type == "object_list":
            convert_object_array_to_csv(
                obj, data["date"], config["columns"], file_path)