var express = require("express");
var app = express();
app.use(express.static('public'))

const fs = require('fs')


// file is included here:
eval(fs.readFileSync('./public/geotools.js')+'');

eval(fs.readFileSync('./public/my-haversine.js')+'');

app.get("/url", (req, res, next) => {
    res.json(["this","is","my","test","server"]);
});

app.listen(3004, () => {
         console.log("Server running on port 3004");
});

