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
es = Elasticsearch(
    ["https://localhost:9200"],
    http_auth=("elastic", "VpTrxCb8lAe+JFiz+gCU"),
    verify_certs=False,
)
mapping = {
                               'mappings': {
                                   'properties': {
                                       'location': {
                                           'type': 'geo_point'
                                       }
                                   }
                               }
                           }
es.indices.create(index='doctor_locations', ignore=400, body=mapping)
mysql_host = "localhost"
mysql_database = "development"
mysql_user = "root"
mysql_password = "root"

sql = "select dl.*, ST_X(dlp.pt) as lat, ST_Y(dlp.pt) as lon  from doctor_location dl"
sql += " join doctor_location_point dlp on dlp.location_id = dl.LOCATION_ID"


mysqldb = mysql.connector.connect(
    host=mysql_host, database=mysql_database, user=mysql_user, password=mysql_password
)
mycursor = mysqldb.cursor(dictionary=True)
mycursor.execute(sql)
myresult = mycursor.fetchall()

rows = myresult
print(
    "Time taken to import data into dataframe: ",
    time.time() - start,
    "rows: ",
    len(rows),
)
start = time.time()
actions = []
i = 0
for item in rows:
    i = i + 1
    action = {
        "_id": i,
        "doc_type": "doctor_locations",
        "LOCATION_ID": item["LOCATION_ID"],
        "DOCTOR_ID": item["DOCTOR_ID"],
        "group_id": item["group_id"],
        "NAME": item["NAME"],
        "ADDRESS_LINE_1": item["ADDRESS_LINE_1"],
        "ADDRESS_LINE_2": item["ADDRESS_LINE_2"],
        "CITY": item["CITY"],
        "STATE": item["STATE"],
        "ZIP_CODE": item["ZIP_CODE"],
        "PHONE_NUMBER": item["PHONE_NUMBER"],
        "FAX_NUMBER": item["FAX_NUMBER"],
        "enabled": item["enabled"],
        "calendarable": item["calendarable"],
        "displayable": item["displayable"],
        "doctor_location_uuid": item["doctor_location_uuid"],
        "TELEMEDICINE": item["TELEMEDICINE"],
        "location": {"lat": item["lat"], "lon": item["lon"]}
    }
    actions.append(action)

response = helpers.bulk(es, actions, index="doctor_locations")

mysqldb.close()
print("Time taken: ", time.time() - start)
