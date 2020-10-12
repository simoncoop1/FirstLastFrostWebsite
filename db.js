const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

// Connection URL
const url = 'mongodb://localhost/test';

const client = new MongoClient(url,{ useUnifiedTopology: true });

client.connect(function(err) {
    assert.equal(null, err);
    console.log("Connected successfully to server");

    const db = client.db('test');

    //findObservations(db,function(){
    GetFirstYear(db,"",function(results){
        LastYear(db,"",results['startdate'],function(){
            client.close();
        });
    });

});

const findObservations = function(db, callback) {
  // Get the documents collection
  const collection = db.collection('observations');
  // Find some documents
    collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"}).sort({"ob_end_time":1}).toArray(function(err, docs) {
    //assert.equal(err, null);
    //console.log("Found the following records");
    //console.log(docs)
    callback(docs);
  });
}

const GetFirstYear = function(db, station, callback){
    const collection = db.collection('observations');

    collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"}).sort({"ob_end_time":1}).next(function(err, docs){
        let startDate = docs['ob_end_time'];                        
        console.log(`the first date is ${startDate.toJSON()}`);
        callback(startDate)    
    });
    
}

const LastYear = function(db,station,results,callback){

    const collection = db.collection('observations');
    collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"}).sort({"ob_end_time":-1}).next(function(err, docs){
        let endDate  = docs['ob_end_time'];                        
        console.log(`the last date is ${endDate.toJSON()}`);
        callback(endDate);   
    });
};

const createYearArray = function(db,station,results,callback){
}

const createFirstLastFrostDictionaryArray = function(){
}


//db.observations.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"}).sort({"ob_end_time":1})

// Use connect method to connect to the Server
/*MongoClient.connect(url,{ useUnifiedTopology: true } , function(err, client) {
    const db = client.db("test");


    console.log("connected successfully to server");

   // db.collection('inventory').find().then(function(result){
   //     console.log(result);
   // })
    //
    const collection = db.collection('observations');
    collection.find({}).toArray(function(err, result){
        assert.equal(null,err);
        console.log("found the following records");
        console.log(results);

    });


    assert.equal(null, err);
    client.close();
});*/






