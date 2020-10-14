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
        LastYear(db,"",results,function(){
            CreateYearArray(db,"",results,function(){
                client.close();
            });
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
        callback({"startDate":startDate})    
    });
    
}

const LastYear = function(db,station,results,callback){

    const collection = db.collection('observations');
    collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"}).sort({"ob_end_time":-1}).next(function(err, docs){
        let endDate  = docs['ob_end_time'];                        
        console.log(`the last date is ${endDate.toJSON()}`);
        results['endDate'] = endDate;
        callback(results);   
    });
};

const GetStationNames = function(db,callback){
    const collection = db.collection('stations');
    collection.find().project({"station_name" :1}).each(function(err,doc){
        if(doc){
        } else{
            //each will keep getting docs until none left of query, and then
            //this else block is run.
        }

    });
}

const CreateYearArray = function(db,station,results,callback){
    //we are looking for full years 1-jul-year1/30-jun-year2
    //that way the first and last fost is calculated from this range
    //this makes sense because it covers autumn,winter,spring, the seasons
    //where relavent data can happen.
    let startDate = results['startDate'];
    let endDate = results['endDate'];
    let startYear = startDate.getFullYear();
    if (startDate.getMonth() >= 6){
        //we move the start year forward one
        startYear += 1;
    }
    let endYear = endDate.getFullYear();
    if (endDate.getMonth() <6){
        endYear -=1;
    }
    let theYears = range(endYear-startYear,startYear);
    callback({"years":theYears});
}

const createFirstLastFrostDictionaryArray = function(db,station results,callback){


}



function range(size, startAt = 0) {
        return [...Array(size).keys()].map(i => i + startAt);
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






