# Cowin Data Tables 
## Raw Tables

- ### states
        description:
        Consists of state id and state name              

        columns: 
        id, name

        primary keys: 
        id

- ### districts

        description:
        Consists of district id and district name of 
        each state              

        columns: 
        id, name, state_id

        primary keys: 
        id

- ### raw_vaccination_site_count

        description:
        Details about number of total, government and 
        private sites             

        columns: 
        total, govt, pvt, today, location_type, 
        location_id, date

        primary keys: 
        location_type, location_id, date
                
- ### raw_registration_count

        description:
        Details about number of people who registered            

        columns:
        total, male, female, others,
        online, onspot, today, flwAndHcw 
        location_type, location_id, date

        primary keys:
        location_type, location_id, date
                
- ### raw_vaccination_session_count

        description:
        Details about sessions on total, government 
        and private sites             

        columns:
        total, govt, pvt, today, location_type, 
        location_id, date

        primary keys:
        location_type, location_id, date
            
- ### raw_vaccination_count

        description:
        Details about number of people who got
        vaccinated             

        columns:
        total, male, female, others, covishield, 
        covaxin, today, tot_dose_1, tot_dose_2, 
        total_doses, aefi, location_type, location_id, 
        date
    
        primary keys:
        location_type, location_id, date
               
- ### raw_vaccination_by_age

        description:
        Details about number of people who got 
        vaccinated based on age group             

        columns:
        total, vac_18_25, vac_25_40, vac_40_60, 
        above_60, location_type, location_id, date

        primary keys:
        location_type, location_id, date

- ### raw_session_vaccination_count

        description:
        Details about vaccinations done based on 
        timestamps    

        columns:
        ts, timestamps, label, count, dose_one, 
        dose_two, location_type, location_id, date

        primary keys:
        location_type, location_id, date, label
                
- ### raw_meta

        description:
        Details on aefiPercentage and timestamp    

        columns:
        timestamp, aefiPercentage, location_type, 
        location_id, date

        primary keys:
        location_type, location_id, date
                
- ### raw_state_level_vaccination_count

        description:
        Details on vaccination based on states    

        columns:
        state_id, id, title, state_name
        total, partial_vaccinated, 
        totally_vaccinated, today, date

        primary keys:
        state_id, date
    
- ### raw_district_level_vaccination_count

        description:
        Details on vaccination based on districts of each 
        state

        columns:
        state_id, state_name, district_id, 
        id, title, district_name, total, 
        partial_vaccinated, totally_vaccinated,
        today, date

        primary keys:
        district_id, date
        
- ### raw_site_level_vaccination_count:

        description:
        Details on vaccination based on site

        columns:
        session_site_id, title, session_site_name, 
        total, partial_vaccinated, totally_vaccinated, 
        today, location_type, location_id, date

        primary keys:
        location_type, location_id, date, session_site_id

## Final Tables

- ### final_vaccination_age

        description:
        Details on vaccination based on age 
        group(national level) extracted from 
        raw_vaccination_by_age

        columns:
        type, national, date

        primary keys:
        type, date

- ### final_state_vaccination_age

        description:
        Details on vaccination based on age 
        group(state level) extracted from 
        raw_vaccination_by_age

        columns:
        type, state, state_id, state_name, date

        primary keys:
        type, date, state_id

- ### final_district_vaccination_age

        description:
        Details on vaccination based on age 
        group(district level) extracted from 
        raw_vaccination_by_age

        columns:
        type, district, district_id, district_name, date

        primary keys:
        type, date, district_id        

- ### national_vaccine_trend

        description:
        Details on vaccination based on date and 
        dose, extracted from raw_vaccination_count

        columns:
        type, national, date

        primary keys:
        type, date

- ### vaccine_data    

        description:
        Details on vaccination based on type of 
        vaccine(national level), extracted from 
        raw_vaccination_count

        columns:
        type, national, date

        primary keys:
        type, date 

- ### state_vaccine_data    

        description:
        Details on vaccination based on type of 
        vaccine(state level), extracted from 
        raw_vaccination_count

        columns:
        type, state, state_id, state_name, date

        primary keys:
        type, date, state_id

- ### district_vaccine_data

        description:
        Details on vaccination based on type of
        vaccine(district level), extracted from 
        raw_vaccination_count

        columns:
        type, district, district_id, district_name, date

        primary keys:
        type, date, district_id




