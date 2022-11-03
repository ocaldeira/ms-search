from sqlalchemy import create_engine
import mysql.connector
from typing import Optional, Tuple
import shapely.geometry
import time
import json
import pandas as pd

start = time.time()
def valid_lonlat(lon: float, lat: float) -> Optional[Tuple[float, float]]:
        """
        This validates a lat and lon point can be located
        in the bounds of the WGS84 CRS, after wrapping the
        longitude value within [-180, 180)

        :param lon: a longitude value
        :param lat: a latitude value
        :return: (lon, lat) if valid, None otherwise
        """
        # Put the longitude in the range of [0,360):
        lon %= 360
        # Put the longitude in the range of [-180,180):
        if lon >= 180:
            lon -= 360
        lon_lat_point = shapely.geometry.Point(lon, lat)
        lon_lat_bounds = shapely.geometry.Polygon.from_bounds(
            xmin=-180.0, ymin=-90.0, xmax=180.0, ymax=90.0
        )

        return lon_lat_bounds.intersects(lon_lat_point)

mysql_host = "localhost"
mysql_database = "development"
mysql_user = "root"
mysql_password = "root"

sql = "select ST_X(pt) as lat, ST_Y(pt) as lon  from doctor_location_point"


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
valid = 0
invalid = 0
for item in rows: 
          if item["lat"] < -90 or item["lat"] > 90 or item["lon"] < -180 or item["lon"] > 180:
                invalid = invalid + 1
                print("invalid", item["lat"], item["lon"])
          else:
                valid = valid + 1

 


print(
    "Time taken to validate data: ",
    time.time() - start,
    "rows: ",
    len(rows),
    "valid: ",
    valid,
    "invalid: ",
    invalid
)

