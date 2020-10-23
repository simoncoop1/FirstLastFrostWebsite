const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

// Connection URL
const url = 'mongodb://localhost/test';

const client = new MongoClient(url,{ useUnifiedTopology: true });

client.connect(function(err) {
    assert.equal(null, err);
    console.log("Connected successfully to server");

    const db = client.db('test');

    //[{"station_file_name":"berriedale-langwell"},{"station_file_name":"foula"}]

    StationNamesQuery(db)
        .then((stations) => {
            let promises =stations.map(doc => {
                let env = {"firstFrost":[],"lastFrost":[],"stationName":doc['station_file_name'],"db":db,"dateCoverMin":0.8};
                let forAStation = GetFirstYearQuery(env)
                    .then(resolve => GetFirstYearHandler(resolve,env))
                    .then(resolve => LastYearQuery(env))
                    .then(resolve => LastYearHandler(resolve,env))
                    .then(resolve => CreateYearArrayQuery(env))
                    .then(resolve => CreateYearArrayHandler(env))
                    .then(resolve => DateRangeDataCoverQuery(env))
                    .then(resolve => DateRangeDataCoverHandler(resolve,env))
                    .then(resolve => FirstFrostTableQuery(env))
                    .then(resolve => FirstFrostTableHandler(resolve,env))
                    .then(resolve => LastFrostTableQuery(env))
                    .then(resolve => LastFrostTableHandler(resolve,env))
                    .then(resolve => PrintResult(env))
                    .then(resolve => AddDB(env))

                return forAStation;
            });
            return Promise.all(promises);
        })
        .then(resolve => client.close())
        .catch(err => console.error(`the is an error ${err} ${err.stack}`));

});


/*const findObservations = function(db, callback) {
  // Get the documents collection
  const collection = db.collection('observations');
  // Find some documents
    collection.find({"min_air_temp":{"$lte":0},"observation_station":"berriedale-langwell"}).sort({"ob_end_time":1}).next(function(err, docs) {
    //assert.equal(err, null);
    //console.log("Found the following records");
    console.log(docs)
    callback(docs);
  });
}*/

const GetFirstYearQuery = function(env){
    const collection = env['db'].collection('observations');
    const station = env['stationName'];
    return collection.find({"min_air_temp":{"$lte":0},"observation_station":station}).sort({"ob_end_time":1}).next()
}

const GetFirstYearHandler = (doc,env) => {
    env['startDate'] = doc['ob_end_time']
    console.log(JSON.stringify(env['startDate']));
    return env;
}

const LastYearQuery = env =>{
    const collection = env['db'].collection('observations');
    const station = env['stationName'];
    return collection.find({"min_air_temp":{"$lte":0},"observation_station":station})
        .sort({"ob_end_time":-1}).next();
};

const LastYearHandler = (doc,env) => {
        env['endDate'] = doc['ob_end_time'];
        console.log(`the last date is ${env['endDate'].toJSON()}`);
        return env;
}

const StationNamesQuery = function(db){
    const collection = db.collection('stations');
    return collection.find().project({"station_file_name" :1}).toArray()
}

const CreateYearArrayQuery = db => {
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
const DateRangeDataCoverQuery = (env)=>{
    const collection = env['db'].collection('observations');
    const station = env['stationName'];
    const dateCoverMin = env['dateCoverMin'];
    const years = env['years']
    console.log(JSON.stringify(env['years']));
    return Promise.all(years.map(x => {
        const promise1 = collection.aggregate([
        {'$group':
            {'_id':station
                ,'hourOfObservation':{'$sum':{'$cond':{'if':
                    {'$and':
                        [{'$lt':['$ob_end_time',new Date(`${x+1}-06-30`)]}
                            ,{'$gte':['$ob_end_time',new Date(`${x}-07-01`)]}
                            ,{'$eq':['$observation_station',station]}]}
                    ,then:'$ob_hour_count'
                    ,else:0
                }}}}}
            ,{'$project':{hourOfObservation:1,year:{$literal:x}}},{'$match':{hourOfObservation:{'$gt':6820}}}
        ]).next();
        return promise1;
    }));
};

const DateRangeDataCoverHandler = (resolve,env)=>{
    console.log("DateRangeDataCoverHandler")
    console.log(JSON.stringify(resolve));
    let removeNull = resolve.filter(record => {
        if(record != null)
            return true;
    });

    env['filteredYears'] =removeNull.map(x => x['year']);
    console.log(`filtered years ${env['filteredYears']}`);
    return env['filteredYears'];
}

const FirstFrostTableQuery =  function(env){
    const collection = env['db'].collection('observations');
    const years = env['filteredYears'];
    const station = env['stationName'];
    console.log(`firstfronttablequery ${env['filteredYears']}`);
    
    return Promise.all(years.map(x => {
        const promise1 = collection.find({"min_air_temp":{"$lte":0},"observation_station":station,
            "ob_end_time":{"$lt":new Date(`${x+1}-07-01T00:00:00Z`),"$gte":new Date(`${x}-07-01T00:00:00Z`)}})
            .sort({"ob_end_time":1}).next();
        return promise1;
    }));
} 

const FirstFrostTableHandler = (doc,env) =>{
    console.log("FirstFrostTableHandler");
	//console.log(JSON.stringify(doc));
	//console.log("test");
	//assert.equal(err, null);
    doc.forEach((x,indx) =>{
        env['firstFrost'].push({year:env['filteredYears'][indx],"document":x});
    });
	return env['filteredYears'];
}

const LastFrostTableQuery =  function(env){
    const collection = env['db'].collection('observations');
    const years = env['filteredYears'];
    const station = env['stationName'];
    return Promise.all(years.map(year => {
        const promise1 = collection.find({"min_air_temp":{"$lte":0},
            "observation_station":station,
            "ob_end_time":{"$lt":new Date(`${year+1}-07-01T00:00:00Z`),"$gte":new Date(`${year}-07-01T00:00:00Z`)}})
            .sort({"ob_end_time":-1}).next();
        return promise1
    }));
}

const LastFrostTableHandler = (doc,env) =>{
    //assert.equal(err,null);
    doc.forEach((x,indx) =>{
        env['lastFrost'].push({year:env['filteredYears'][indx],"station_file_name":env['stationName'],"document":x});
    });
    return env['filteredYears'];
}

const PrintResult = (env) => {
    console.log(`last frost ${JSON.stringify(env['lastFrost'])}`);
    console.log(`first frost ${JSON.stringify(env['firstFrost'])}`);
}

const AddDB = (env) => {
    const collection = env['db'].collection('firstFrost');
    const promise1 =  collection.insertMany(env['firstFrost']);

    const collection2 = env['db'].collection('lastFrost');
    const promise2 = collection2.insertMany(env['lastFrost']);

    return Promise.all([promise1,promise2])
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






