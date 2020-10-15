const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

// Connection URL
const url = 'mongodb://localhost/test';

const client = new MongoClient(url,{ useUnifiedTopology: true });

client.connect(function(err) {
    assert.equal(null, err);
    console.log("Connected successfully to server");

    const db = client.db('test');

    let env = {"firstFrost":[],"lastFrost":[]};

    /*findObservations(db,function(){
        client.close();
    });*/
    
    GetFirstYear(db,env, function(env){
        LastYear(db,env, function(env){
            CreateYearArray(db,env, function(env){
                createFirstLastFrostDictionaryArray(db,env, function(env){
                    client.close();
                });
            });
        });
    });

});

const findObservations = function(db, callback) {
  // Get the documents collection
  const collection = db.collection('observations');
  // Find some documents
    collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"}).sort({"ob_end_time":1}).next(function(err, docs) {
    //assert.equal(err, null);
    //console.log("Found the following records");
    console.log(docs)
    callback(docs);
  });
}

const GetFirstYear = function(db,env,callback){
    const collection = db.collection('observations');

    collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"}).sort({"ob_end_time":1}).next(function(err, docs){
        env['startDate'] = docs['ob_end_time'];
        let startDate = env['startDate'];
        console.log(`the first date is ${startDate.toJSON()}`);
        callback(env)    
    });
    
}

const LastYear = function(db,env,callback){

    const collection = db.collection('observations');
    collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"}).sort({"ob_end_time":-1}).next(function(err, docs){
        let endDate  = docs['ob_end_time'];                        
        console.log(`the last date is ${endDate.toJSON()}`);
        env['endDate'] = endDate;
        callback(env);   
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

const CreateYearArray = function(db,env,callback){
    //we are looking for full years 1-jul-year1/30-jun-year2
    //that way the first and last fost is calculated from this range
    //this makes sense because it covers autumn,winter,spring, the seasons
    //where relavent data can happen.
    let startDate = env['startDate'];
    let endDate = env['endDate'];
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
    env['years'] = theYears;
    console.log(env['years']);
    callback(env);
}

const createFirstLastFrostDictionaryArray = function(db,env,callback){

    const collection = db.collection('observations');

    let year = env['years'].pop();

    if(year == undefined){
        console.log(JSON.stringify(env));
        callback(env);
    }
    else{
        console.log(JSON.stringify({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell",
                "ob_end_time":{"$lt":new Date(`${year+1}-07-01T00:00:00Z`),"$gte":new Date(`${year}-07-01T00:00:00Z`)}}));
        collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell",
        "ob_end_time":{"$lt":new Date(`${year+1}-07-01T00:00:00Z`),"$gte":new Date(`${year}-07-01T00:00:00Z`)}})
            .sort({"ob_end_time":1}).next(function(err, docs){
                console.log("test");
                assert.equal(err, null);
                console.log(`the first frost ${docs['ob_end_time']}`);
                //console.log(JSON.stringify(docs));
                //console.log(JSON.stringify(env));
                env['firstFrost'].push({"year":year,"document":docs});
                collection.find({"min_air_temp":{"$lte":0},
                    "observation_station":"berriedale-langwell",
                    "ob_end_time":{"$lt":new Date(`${year+1}-07-01T00:00:00Z`),"$gte":new Date(`${year}-07-01T00:00:00Z`)}})
                    .sort({"ob_end_time":-1}).next(function(err, docs){
                            assert.equal(err,null);
                            env['lastFrost'].push({"year":year,"document":docs});
                            createFirstLastFrostDictionaryArray(db,env,callback);
                        });
            });
    }
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






