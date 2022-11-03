from sqlalchemy import create_engine
import mysql.connector  
from typing import Optional, Tuple
import shapely.geometry
import time
import json
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers 
start = time.time()
es = Elasticsearch (['https://localhost:9200'], http_auth=('elastic', 'VpTrxCb8lAe+JFiz+gCU'), verify_certs=False)
mapping = {
                               'mappings': {
                                   'properties': {
                                       'location': {
                                           'type': 'geo_point'
                                       }
                                   }
                               }
                           }
es.indices.create(index='insurances_by_location', ignore=400, body=mapping)

mysql_host="localhost"
mysql_database="development"
mysql_user="root"
mysql_password="root"

SQL = 'select di.DOCTOR_INSURANCE_ID,di.DOCTOR_ID,di.INSURANCE_ID,di.PLAN_ID,ST_X(dlp.pt) as lat, ST_Y(dlp.pt) as lon , i.name as insurance_name, i.insurance_uuid from doctor_insurance di'
SQL += ' join doctor_location dl on dl.DOCTOR_ID = di.DOCTOR_ID'
SQL += ' join insurance i on i.INSURANCE_ID = di.INSURANCE_ID'
SQL += ' join doctor_location_point dlp on dlp.location_id = dl.LOCATION_ID'
SQL += ' where di.INSURANCE_ID is not null'
SQL += ' and di.PLAN_ID is not null'
SQL += ' LIMIT 3000000'


mysqldb = mysql.connector.connect(
    host=mysql_host,
    database=mysql_database,
    user=mysql_user,
    password=mysql_password
)
mycursor = mysqldb.cursor(dictionary=True)
mycursor.execute(SQL)
myresult = mycursor.fetchall()

rows = myresult 
print("Time taken to import data into dataframe: ", time.time() - start, "rows: ", len(rows))
start = time.time()
actions=[]
i = 0
for item in rows:
    i = i + 1
    action = {
        "_id": i,
        "doc_type": "insurance",
        "_source": {
            "doctor_id": item['DOCTOR_ID'],
            "insurance_id": item['INSURANCE_ID'],
            "plan_id": item['PLAN_ID'],
            "insurance_name": item['insurance_name'],
            "insurance_uuid": item['insurance_uuid'],
            "location": {
                "lat": item['lat'],
                "lon": item['lon']
            }
        }
    }
    actions.append(action)
 
response = helpers.bulk(es, actions, index="insurances_by_location")

mysqldb.close()
print("Time taken: ", time.time() - start)