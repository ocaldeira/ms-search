const { Client } = require("@elastic/elasticsearch");
const config = require("config");
const elasticConfig = config.get("elastic");
console.log(elasticConfig);
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

controller.getInsurances = async (req, res) => {
  try {
    lat = req.query.lat;
    long = req.query.long;
    doctor_id = req.query.doctor_id;
    distance = req.query.distance;
    const pipeline = {
      index: "insurances_by_location",
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
      size: 1000,
    };

    
    try {
      const insurances = await client.search(pipeline);
      return insurances;
    } catch (error) {
      return error;
    }
  } catch (error) {
    return error;
  }
};

module.exports = controller;
