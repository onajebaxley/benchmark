'use strict';

const MongoClient = require('mongodb').MongoClient;

const uri = 'mongodb://mongo-0.mongo,mongo-1.mongo,mongo-2.mongo:27017';

MongoClient.connect(uri, testDb);

function testDb(err, client) {
    if (err) {
        console.log(`ERROR connecting to ${uri}`);
        console.log(err.message);
        return;
    }

    const dbName = 'test';
    const db = client.db(dbName);

    // check server config
    const config = db.serverConfig;
    console.log(`db server config: ${JSON.stringify(config)}`);

    let numRecords = 1000;
    let hrTime = process.hrtime();
    let startTime = hrTime[0] * 1000000000 + hrTime[1]; // start in sE(-9)

    // TODO: Insert 1000 records

    hrTime = process.hrtime();
    let endTime = hrTime[0] * 1000000000 + hrTime[1]; // end time in sE(-9)
    let nanoDiff = endTime - starTime;

    console.log(`Took ${nanoDiff} seconds to insert ${numRecords} records.`);

    client.close();
}

module.exports.testDb = testDb;

