const { Client } = require("@elastic/elasticsearch");
const config = require("config");
const elasticConfig = config.get("elastic");
const client = new Client({
  auth: {
    username: elasticConfig.username,
    password: elasticConfig.password,
  },
  node: elasticConfig.node,
  log: "trace",
  tls: { rejectUnauthorized: false }
});

let controller = {};

controller.getDoctorLocations = async (req, res) => {
  try {
    lat = req.query.lat;
    long = req.query.long;
    distance = req.query.distance;
    const pipeline = {
      index: "doctor_locations",
      query: {
        bool: {
          filter: {
            geo_distance: {
              distance: distance + "km",
              location: {
                lat: lat,
                lon: long,
              },
            },
          },
        },
      },
      aggs: {
        locations: {
          terms: {
            field: "doctor_id",
          },
        },
      },
      sort:{
         "_geo_distance":{
            "location":{
                "lat":lat,
                "lon":long
            },
            "order":"asc",
            "unit":"km"
          }
      },
      _source: false,
      fields: ["DOCTOR_ID", "location", "distance", "distance_unit","ADDRESS_LINE_1","ADDRESS_LINE_2","CITY","STATE","ZIP_CODE","COUNTRY","distance"],

      size: 1000,
    };

    try {
      const locations = await client.search(pipeline);
      return locations;
    } catch (error) {
      return error;
    }
  } catch (error) {
    return error;
  }
};

module.exports = controller;
