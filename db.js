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

    GetFirstYearQuery(db)
        .then(resolve => GetFirstYearHandler(resolve['ob_end_time'],env))
        .then(resolve => LastYearQuery(db))
        .then(resolve => LastYearHandler(resolve['ob_end_time'],env))
        .then(resolve => CreateYearArrayQuery(db))
        .then(resolve => CreateYearArrayHandler(env))
        .then(resolve => DateRangeDataCoverQuery(db,"","",resolve,env))
        .then(resolve => DateRangeDataCoverHandler())
        .then(resolve => FirstFrostTableQuery(db,env))
        .then(resolve => FirstFrostTableHandler())
        .then(resolve => LastFrostTableHandler())
        .then(resolve => LastFrostTableHandler())
        .then(resolve => client.close())
        .catch(err => console.error(`the is an error ${err} ${err.stack}`));

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

const GetFirstYearQuery = function(db){
    const collection = db.collection('observations');
    return collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"}).sort({"ob_end_time":1}).next()
}

const GetFirstYearHandler = (startDate,env) => {
    env['startDate'] = startDate;
    console.log(JSON.stringify(startDate));
    return env;
}

const LastYearQuery = db =>{
    const collection = db.collection('observations');
    return collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"})
        .sort({"ob_end_time":-1}).next();
};

const LastYearHandler = (endDate,env) => {
        env['endDate'] = endDate;
        console.log(`the last date is ${endDate.toJSON()}`);
        return env;
}

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

const CreateYearArrayQuery = db => {
    return;
}

const CreateYearArrayHandler =  function(env){
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
    return theYears;
}

//want to have data for a complete year
const DateRangeDataCoverQuery = (db,station,threshhold,years,env)=>{
    const collection = db.collection('observations');
    return Promise.all([1984,1983].map(x => {
        const promise1 = collection.aggregate([
        {'$group':
            {'_id':station
                ,'hourOfObservation':{'$sum':{'$cond':{'if':
                    {'$and':
                        [{'$lt':['$ob_end_time',new Date(`${x+1}-06-30`)]}
                            ,{'$gte':['$ob_end_time',new Date(`${x}-07-01`)]}
                            ,{'$eq':['$observation_station','berriedale-langwell']}]}
                    ,then:'$ob_hour_count'
                    ,else:0
                }}}}}
            ,{'$project':{hourOfObservation:1,year:JSON.stringify(x)}},{'$match':{hourOfObservation:{'$gt':6820}}}
        ])
            .next();
        return promise1;
    }));
};

const DateRangeDataCoverHandler = (resolve,env)=>{

}

const FirstFrostTableQuery =  function(db,env){

    const collection = db.collection('observations');

    let year = env['years'].pop();

    if(year == undefined){
        //console.log(JSON.stringify(env));
        callback(env);
    }
    else{
        console.log(JSON.stringify({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell",
                "ob_end_time":{"$lt":new Date(`${year+1}-07-01T00:00:00Z`),"$gte":new Date(`${year}-07-01T00:00:00Z`)}}));
        collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell",
        "ob_end_time":{"$lt":new Date(`${year+1}-07-01T00:00:00Z`),"$gte":new Date(`${year}-07-01T00:00:00Z`)}})
            .sort({"ob_end_time":1}).next(function(err, docs){
                //console.log("test");
                assert.equal(err, null);
                //console.log(`the first frost ${docs['ob_end_time']}`);
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

    Promise.all(env['years'].map(x => {
        const promise1 = collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell",
            "ob_end_time":{"$lt":new Date(`${year+1}-07-01T00:00:00Z`),"$gte":new Date(`${year}-07-01T00:00:00Z`)}})
            .sort({"ob_end_time":1}).next();
        return promise1;
    }))
} 

const FirstFrostTableHandler = () =>{
    
}

const LastFrostTableQuery =  function(db,env){
}

const LastFrostTableHandler = () =>{
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






