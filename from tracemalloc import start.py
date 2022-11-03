from tracemalloc import start
from typing import Optional, Tuple
import mysql.connector
import pymongo
import json
import shapely.geometry
import time

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

        if lon_lat_bounds.intersects(lon_lat_point):
            return 1
        else:
            return 0

delete_existing_documents = True
mysql_host="51.89.172.136"
mysql_database="rets_data_ui"
mysql_schema = "property_list_search"
mysql_user="sapAdmin"
mysql_password="Amore9384!"

mongodb_host = "mongodb://localhost:27017/"
mongodb_dbname = "rets"

mysqldb = mysql.connector.connect(
    host=mysql_host,
    database=mysql_database,
    user=mysql_user,
    password=mysql_password
)

mycursor = mysqldb.cursor(dictionary=True)
mycursor.execute("SELECT  * from property_list_search   ;")
myresult = mycursor.fetchall()


myclient = pymongo.MongoClient(mongodb_host,
                     username='sapAdmin',
                     password='Amore9384!',
                     authSource='admin',
                     authMechanism='SCRAM-SHA-256')
mydb = myclient[mongodb_dbname]

mycol = mydb["rets_search"]
x = mycol.delete_many({})
finalResult = []
if len(myresult) > 0:
        for row in myresult:
            rowResult = {}
            for col in row:
                rowResult[col] = row[col]
            if row["longitude"] is None or row["latitude"] is None:
                rowResult["coordinates"] = None
            elif valid_lonlat(float(row["longitude"]), float(row["longitude"])) == 1:

                jsonTxt = '{ "type": "Point", "coordinates": [ '+str(row["longitude"])+', '+str(row["latitude"])+' ] }'

                rowResult["coordinates"]=json.loads(jsonTxt)
            else:
                rowResult["coordinates"] = None

                #rowResult["_id"] = row["listing_id"]
            #print(row["listing_id"])
            #mycol.update_one({"listing_id":row["listing_id"]},{"$set":rowResult}, upsert=True)
            finalResult.append(rowResult)

        #print(x.deleted_count, " documents deleted.")
        x = mycol.insert_many(finalResult) #myresult comes from mysql cursor
        #x = mycol.update_many({}, {"$set": finalResult}, upsert=True)
        print(len(x.inserted_ids))

print("Time taken: ", time.time() - start)