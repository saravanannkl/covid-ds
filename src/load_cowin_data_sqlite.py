import os
import io
import logging
import json
import csv
#from posix import listdir
from sqlite3.dbapi2 import Date
from sqlite_utils import Database

DB_FILE_NAME = "covid-ds.db"

# JSON File and table mapping
JSON_TABLE_MAPPING_INFO={
    "national.json":{
        "tables":{
            "raw_vaccination_site_count":{
                "type": "object",
                # "columns": ["total", "govt", "pvt", "today"],
                "path": "topBlock.sites",
                "load_location_columns": True,
                "pk": ("location_type", "location_id", "date")
            },
            "raw_registration_count": {
                "type": "object",
                # "columns": ["total", "cit_18_45", "cit_45_above", "today"],
                "path": "topBlock.registration",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date"
                )
            },
            "raw_vaccination_session_count": {
                "type": "object",
                # "columns": ["total", "govt", "pvt", "today"],
                "path": "topBlock.sessions",
                "load_location_columns": True,
                "pk": ("location_type", "location_id", "date")
            },
            "raw_vaccination_count": {
                "type": "object",
                # "columns": [
                #     "total",
                #     "male",
                #     "female",
                #     "others",
                #     "covishield",
                #     "covaxin",
                #     "today",
                #     "tot_dose_1",
                #     "tot_dose_2",
                #     "total_doses",
                #     "aefi"
                # ],
                "path": "topBlock.vaccination",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date"
                )
            },
            "raw_vaccination_by_age": {
                "type": "object",
                # "columns": ["total", "vac_18_30", "vac_30_45", "vac_45_60", "above_60"],
                "path": "vaccinationByAge",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date"
                )
            },
            "raw_session_vaccination_count": {
                "type": "object_list",
                # "columns": ["ts", "timestamps", "label", "count", "dose_one", "dose_two"],
                "path": "vaccinationDoneByTime",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date",
                    "label"
                )
            },
            "raw_meta": {
                "type": "object",
                "columns": ["timestamp", "aefiPercentage", "date","location_type","location_id"],
                "path": ".",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date"
                )
            },
            "raw_state_level_vaccination_count": {
                "location_type": "national",
                "type": "object_list",
                # "columns": ["state_id", "state_name", "total", "partial_vaccinated", "totally_vaccinated", "today"],
                "path": "getBeneficiariesGroupBy",
                "load_location_columns": False,
                "pk": (
                    "date",
                    "state_id"
                )
            },    
        }
    },
    "state":{
        "tables":{
            "raw_vaccination_site_count":{
                "type": "object",
                # "columns": ["total", "govt", "pvt", "today"],
                "path": "topBlock.sites",
                "load_location_columns": True,
                "pk": ("location_type", "location_id", "date")
            },
            "raw_vaccination_session_count": {
                "type": "object",
                # "columns": ["total", "govt", "pvt", "today"],
                "path": "topBlock.sessions",
                "load_location_columns": True,
                "pk": ("location_type", "location_id", "date")
            },
            "raw_vaccination_count": {
                "type": "object",
                # "columns": [
                #     "total",
                #     "male",
                #     "female",
                #     "others",
                #     "covishield",
                #     "covaxin",
                #     "today",
                #     "tot_dose_1",
                #     "tot_dose_2",
                #     "total_doses",
                #     "aefi"
                # ],
                "path": "topBlock.vaccination",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date"
                )
            },
            "raw_vaccination_by_age": {
                "type": "object",
                # "columns": ["total", "vac_18_30", "vac_30_45", "vac_45_60", "above_60"],
                "path": "vaccinationByAge",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date"
                )
            },
            "raw_session_vaccination_count": {
                "type": "object_list",
                # "columns": ["ts", "timestamps", "label", "count", "dose_one", "dose_two"],
                "path": "vaccinationDoneByTime",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date",
                    "label"
                )
            },
            "raw_meta": {
                "type": "object",
                "columns": ["timestamp", "aefiPercentage", "date","location_type","location_id"],
                "path": ".",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date"
                )
            },
            "raw_district_level_vaccination_count":{
                "location_type": "state",
                "type": "object_list",
                # "columns": ["state_id", "district_id", "district_name", "total", "partial_vaccinated", "totally_vaccinated", "today"],
                "path": "getBeneficiariesGroupBy",
                "load_location_columns": False,
                "pk": (
                    "date",
                    "district_id"
                )
            }    
        }
    },
    "district":{
        "tables":{
            "raw_vaccination_site_count":{
                "type": "object",
                # "columns": ["total", "govt", "pvt", "today"],
                "path": "topBlock.sites",
                "load_location_columns": True,
                "pk": ("location_type", "location_id", "date")
            },
            "raw_vaccination_session_count": {
                "type": "object",
                # "columns": ["total", "govt", "pvt", "today"],
                "path": "topBlock.sessions",
                "load_location_columns": True,
                "pk": ("location_type", "location_id", "date")
            },
            "raw_vaccination_count": {
                "type": "object",
                # "columns": [
                #     "total",
                #     "male",
                #     "female",
                #     "others",
                #     "covishield",
                #     "covaxin",
                #     "today",
                #     "tot_dose_1",
                #     "tot_dose_2",
                #     "total_doses",
                #     "aefi"
                # ],
                "path": "topBlock.vaccination",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date"
                )
            },
            "raw_vaccination_by_age": {
                "type": "object",
                # "columns": ["total", "vac_18_30", "vac_30_45", "vac_45_60", "above_60"],
                "path": "vaccinationByAge",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date"
                )
            },
            "raw_session_vaccination_count": {
                "type": "object_list",
                # "columns": ["ts", "timestamps", "label", "count", "dose_one", "dose_two"],
                "path": "vaccinationDoneByTime",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date",
                    "label"
                )
            },
            "raw_meta": {
                "type": "object",
                "columns": ["timestamp", "aefiPercentage", "date","location_type","location_id"],
                "path": ".",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date"
                )
            },
            "raw_site_level_vaccination_count":{
               "location_type": "district",
                "type": "object_list",
                # "columns": ["session_site_id", "session_site_name", "total", "partial_vaccinated", "totally_vaccinated", "today"],
                "path": "getBeneficiariesGroupBy",
                "load_location_columns": True,
                "pk": (
                    "location_type",
                    "location_id",
                    "date",
                    "session_site_id"
                )
            }    
        }
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

def get_nested_object(obj, path_key, table, table_data_info):
    """
        Gets the value of deeply nested object
        with nested keys separated by dot(.)
        Used to extract nested data from COWIN
        output JSON

        Parameters:
            obj: JSON Object
            path_key: nested key separated by dot(.)
            table: name of the table
            table_data_info: structure of the tables
        Returns:
            Value of the nested key.
            If the nested key is not present, then None is returned.
    """
    if path_key == ".":
        value = {}
        for column in table_data_info[table]["columns"]: 
            value[column] = obj.get(column) 
        return value
                
    parts = path_key.split(".")
    value = obj
    for part in parts:
        value = value.get(part)
        if not value:
            break
    return value

def load_json_file(db, table, json_file_data, table_load_info, location_col_values, pk, path, type):
    """
        Loads JSON file into a table.

        Parameters:
            db: Sqlite database connection
            table: Table name into which the table needs to be loaded.
            json_file_data: json data that consists of records for the table
            table_load_info: structure of the table
            location_col_values: If the columns location_type and location_id
                needs to be added to the table, then that values
            pk: primary keys for the particular table    
            path: path of the data for the table in the JSON response
            type: type of the data in the JSON response, can be either object or object_list
    """
    record = get_nested_object(json_file_data,path,table, table_load_info)
    date = json_file_data.get("date")
    if location_col_values != [] and path != '.' and type != 'object_list':
        record["location_type"] = location_col_values[0]
        record["location_id"] = location_col_values[1]
    if type == 'object_list':
        for data in record:
            if location_col_values != []:    
                data["location_type"] = location_col_values[0]
                data["location_id"] = location_col_values[1]
            data["date"] = date
        db[table].insert_all(record)
    else:
        record["date"] = date    
        db[table].insert(record)
        
    db[table].transform(pk=pk)
   
    
    




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


def load_location_data_files(db, folder, location_type, location_id):
    """
        Loads location data files in a folder into database.

        Parameters:
            db: Sqlite database connection
            folder: Folder that contains JSOn files
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
        if 'states' in folder:
            table_load_info = JSON_TABLE_MAPPING_INFO.get('state')
        elif 'districts' in folder:    
            table_load_info = JSON_TABLE_MAPPING_INFO.get('district')
        else:    
            table_load_info = JSON_TABLE_MAPPING_INFO.get(file)
        if not table_load_info:
            logging.warning("No mapping available for file {}".format(file))
            continue

        logging.info("Processing file {}".format(file))
        json_file_data = get_json_from_file(os.path.join(folder, file))
        for table_name,table_value in table_load_info["tables"].items():
            table = table_name
            pk = table_value.get("pk")
            location_col_values = []
            if table_value.get("load_location_columns"):
                if location_type == 'national': 
                    location_col_values = [location_type, location_id]
                else:
                    location_col_values = [location_type, file.replace(".json","")]    
            json_data_path = table_value.get("path")
            type = table_value.get("type")
            load_json_file(db, table,json_file_data,table_load_info["tables"],
                            location_col_values, pk, json_data_path, type)

def load_folder_data(db, folder, data_path_len):
    path_split = folder.split(os.path.sep)
    data_sub_folders = path_split[data_path_len:]
    data_sub_folder_count = len(data_sub_folders)
    location_type = ""
    location_id = 0
    if data_sub_folder_count == 1:
         location_type = "national"
    elif data_sub_folder_count == 2 and 'states' in folder:
        location_id = [file for file in os.listdir(folder)][0].split('.')[0]
        location_type = "state"
    elif data_sub_folder_count == 2 and 'districts' in folder:
        location_id = [file for file in os.listdir(folder)][0].split('.')[0]
        location_type = "district"
    else:
        raise Exception("Invalid Data folder: {}".format(folder))
    date = data_sub_folders[-1]
    load_location_data_files(db, folder, location_type, location_id)


def load_cowin_data(root_folder):
    """
        Main method to load data into sqlite file

        Parameter:
            root_folder: Root folder of the project
    """
    db_file_path = os.path.join(root_folder, DB_FILE_NAME)
    if os.path.exists(db_file_path):
        os.remove(db_file_path)

    db = Database(db_file_path)
    load_state_district_meta_data(db)
    
    data_folder = os.path.join(root_folder, "data", "cowin")
    data_path_len = len(data_folder.split(os.path.sep))
    for (root, dirs, files) in os.walk(data_folder):
        if "2021-" in root:
            load_folder_data(db, root, data_path_len)
    
    db.conn.close()



def raw_vacc_age_to_final(root_folder):
    """
    Creates a new table final_vaccination_age from raw_vaccination_count in the database

    Parameters:
        root_folder: Root folder of the project
    
    """
    db_file_path = os.path.join(root_folder, DB_FILE_NAME)
    db = Database(db_file_path)
    result = list(db.execute("Select date from raw_vaccination_by_age"))
    max_date = max(result)
    result = list(db.execute("Select * from raw_vaccination_by_age WHERE date= ? and location_type =?",[max_date[0],"national"]))
    for item in result:
        date = max_date[0]
        vac_18_25, vac_25_40, vac_40_60, above_60 = item[1], item[2], item[3], item[4]
        db["final_vaccination_age"].insert_all([{
                "type": "vac_18_25",
                "national": vac_18_25,
                "date": date,    
                },
                {
                "type": "vac_25_40",
                "national": vac_25_40,
                "date": date,    
                },
                {
                "type": "vac_40_60",
                "national": vac_40_60,
                "date": date,    
                },
                {
                "type": "above_60",
                "national": above_60,
                "date": date,    
                }])
    db["final_vaccination_age"].transform(pk=("type","date"))

def raw_vacc_age_to_final_state(root_folder):
    """
    Creates a new table final_state_vaccination_age from raw_vaccination_count in the database

    Parameters:
        root_folder: Root folder of the project
    
    """
    db_file_path = os.path.join(root_folder, DB_FILE_NAME)
    db = Database(db_file_path)
    result = list(db.execute("Select date from raw_vaccination_by_age"))
    max_date = max(result)
    result = list(db.execute("Select * from raw_vaccination_by_age WHERE date= ? and location_type =?",[max_date[0],"state"]))
    for item in result:
        result_state_name = list(db.execute("Select name from states WHERE id= ?",[item[6]]))
        state_name = result_state_name[0][0]
        state_id = item[6]
        date = max_date[0]
        vac_18_25, vac_25_40, vac_40_60, above_60 = item[1], item[2], item[3], item[4]
        db["final_state_vaccination_age"].insert_all([{
                "type": "vac_18_25",
                "state": vac_18_25,
                "state_name":state_name,
                "state_id": state_id, 
                "date": date,    
                },
                {
                "type": "vac_25_40",
                "state": vac_25_40,
                "state_name":state_name,
                "state_id": state_id,
                "date": date,    
                },
                {
                "type": "vac_40_60",
                "state": vac_40_60,
                "state_name":state_name,
                "state_id": state_id,
                "date": date,    
                },
                {
                "type": "above_60",
                "state": above_60,
                "state_name":state_name,
                "state_id": state_id,
                "date": date,    

                }])
    db["final_state_vaccination_age"].transform(pk=("date","state_id","type"))
           
       

def vaccine_data(root_folder):
    """
    Creates a new table vaccine_data from raw_vaccination_count in the database

    Parameters:
        root_folder: Root folder of the project
    
    """
    db_file_path = os.path.join(root_folder, DB_FILE_NAME)
    db = Database(db_file_path)
    result = list(db.execute("Select date from raw_vaccination_count"))
    max_date = max(result)
    result = list(db.execute("Select * from raw_vaccination_count WHERE location_type= ?",["national"]))
    for item in result:
        date = item[13]
        if date == max_date[0]:
            covidshield, covaxin= item[4], item[5]
            db["vaccine_data"].insert_all([{
                    "type": "covidshield",
                    "national": covidshield,
                    "date": date,    
                    },
                    {
                    "type": "covaxin",
                    "national": covaxin,
                    "date": date,    
                    },
                   ]) 
    db["vaccine_data"].transform(pk=("date","type"))         

def state_vaccine_data(root_folder):
    """
    Creates a new table state_vaccine_data from raw_vaccination_count in the database

    Parameters:
        root_folder: Root folder of the project
    
    """
    db_file_path = os.path.join(root_folder, DB_FILE_NAME)
    db = Database(db_file_path)
    result = list(db.execute("Select date from raw_vaccination_count"))
    max_date = max(result)
    result = list(db.execute("Select * from raw_vaccination_count WHERE location_type= ?",["state"]))
    for item in result:
        date = item[13]
        result_state_name = list(db.execute("Select name from states WHERE id= ?",[item[12]]))
        state_name = result_state_name[0][0]
        state_id = item[12]
        if date == max_date[0]:
            covidshield, covaxin = item[4], item[5]
            db["state_vaccine_data"].insert_all([{
                    "type": "covidshield",
                    "state": covidshield,
                    "state_name":state_name,
                    "state_id": state_id,
                    "date": date,    
                    },
                    {
                    "type": "covaxin",
                    "state": covaxin,
                    "state_name":state_name,
                    "state_id": state_id,
                    "date": date,    
                    },
                  ]) 
    db["state_vaccine_data"].transform(pk=("date","state_id","type"))                         
     
    

def national_vaccine_trend(root_folder):
    """
    Creates a new table national_vaccination_count from existing table raw_vaccination_count in the database

    Parameters:
        root_folder: Root folder of the project
    
    """
    db_file_path = os.path.join(root_folder, DB_FILE_NAME)
    db = Database(db_file_path)
    result = list(db.execute("Select * from raw_vaccination_count WHERE location_type= ?",["national"]))
    for item in result:
        date = item[13]
        today, dose1, dose2 = item[6], item[7], item[8]
        db["national_vaccine_trend"].insert_all([{
            "type": "today",
            "national": today,
            "date": date,    
            },
            {
            "type": "Dose 1",
            "national": dose1,
            "date": date,    
            },
            {
            "type": "Dose 2",
            "national": dose2,
            "date": date,    
            }])
    db["national_vaccine_trend"].transform(pk=("date","type"))    



if __name__ == "__main__":
    folder = os.path.dirname(os.path.abspath(__file__))
    load_cowin_data(folder)
    raw_vacc_age_to_final(folder)
    raw_vacc_age_to_final_state(folder)
    vaccine_data(folder)
    state_vaccine_data(folder)
    national_vaccine_trend(folder)
