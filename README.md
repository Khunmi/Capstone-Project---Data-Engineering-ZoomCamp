## Capstone Project: Batch Processing- No-Show Appointment Dataset

## Documentation and Project Files
All files for peer review are contained within

## Project Description
This project seeks to investigate hospital appointments no-show trends. is attendance a function of age, ailment type or proximity to the hospital? It may also be that SMS prompt play a vital role in helping people honor hospital appointments. 

    ## Questions for Analysis
    * What age ranges are more likely to honor appointments as well as the ages that most likely wouldn't?
    * Does SMS prompts play an important role in appoinment showup rates?
    * What percentage of males and females are enrolled in welfare programs?
    
## Objective 
To satisfy an end to end data engineering life cycle.

### Files and Tools used
* GCS - google cloud storage for data lake

* capstone_proj.py - This script reads csv data from GCS and transforms it using Spark. Finally it writes the output dataset to a table called "capstone2023" as a Parquet file in Bigquery

* Pyspark - Triggered Spark jobs within Google cloud storage using the following code snippet via the command line interface 

''' gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=northamerica-northeast2 \
    --project=khunmi-academy-376002 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc_data_lake_khunmi-academy-376002/code/capstone_proj.py \
    -- \
        --input_data=gs://dtc_data_lake_khunmi-academy-376002/capstone/ \
        --output=trips_data_all.capstone2023 '''

* Google Data Studio: To generate insights from my dataset, I connected my data source located in my data warehouse(Bigquery). Have a look at my dashboard link here https://lookerstudio.google.com/reporting/d7164b60-c4ee-4c65-937d-d10a23b1a75d

![image](https://user-images.githubusercontent.com/95039004/236275008-5d965704-c5c9-4186-b657-1de3bced87bd.png)



### Dataset

This dataset collects information from 100k medical appointments in Brazil and is focused on the
question of whether or not patients show up for their appointment. A number of characteristics
about the patient are included in each row.‘ScheduledDay’ tells us on what day the patient setup their appointment.‘Neighborhood’ indicates the location of the hospital.‘Scholarship’ indicates whether or not the patient is enrolled in Brasilian welfare program Bolsa Família:

        Variable                Data_ Type
    1. PatientId                   float64
    2. Appointment                 IDint64
    3. Gender                       object
    4. ScheduledDay                 object
    5. AppointmentDay               object
    6. Age                           int64
    7. Neighbourhood                object
    8. Scholarship                   int64
    9. Hipertension                  int64
    10. Diabetes                     int64
    11. Alcoholism                   int64
    12. Handcap                      int64
    13. SMS_received                 int64
    14. No-show                     object



## Other files in this repo details on interacting with ETL processes and getting comfortable with orchestrating, Ifrastructure as code and many others.
