const express = require("express"); 
const Router = require("./routes/routes") 
const app = express();  
 

app.use(express.json());
 
app.use('/api', Router)
app.listen(3402, () => {
    console.log(`Server Started at ${3402}`)
})