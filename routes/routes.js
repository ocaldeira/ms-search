const express = require("express");
const router = express.Router();
const DoctorLocations = require("../src/controllers/doctor_locations");
const InsurancesByLocation = require("../src/controllers/insurance_by_location");

module.exports = router;

router.get("/getInsurances", async (req, res) => {
    let params = req;  
    try {
        const insurances = await InsurancesByLocation.getInsurances(params);
        console.log(insurances);
        res.status(200).json([{count:insurances.length, message:"success", status:200,data:insurances}]);
    }
    catch (error) {
        res.status(500).json({ message: error.message });
    }

 

  
});

router.get("/getDoctorLocations", async (req, res) => {
    let params = req;  
    try {
        const doctorLocations = await DoctorLocations.getDoctorLocations(params);
        res.status(200).json([{count:doctorLocations.length, message:"success", status:200,data:doctorLocations}]);
    } catch (error) {
        res.status(500).json({ message: error.message });
    }

    
    });

