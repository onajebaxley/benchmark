'use strict';

const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const data = fs.readFileSync('./demographics.csv');

function parseCsvData(dataFile) {
    let dataArr = bufferString.toString().split('\n');
    let headers = dataArr[0].toLowerCase().split(',');
    let fields = [];

    // Remove erroneous characters from headers
    for (let i = 0; i < headers.length; i++) {
        let refined = headers[i].replace(/[\W_]+/g,"_");

        fields.push(refined);
    }

    // Populate array of objects from CSV
    const formattedObjects = [];
    for(let i = 1; i < dataArr.length - 1; i++) {
        let data = dataArr[i].split(',');
        let obj = {};
        for(let j = 0; j < data.length; j++) {
            obj[fields[j].trim()] = data[j].trim();
        }

        formattedObjects.push(obj);
    }

    return formattedObjects;
}

// console.log(JSON.stringify(sampleData[235]));

const uri = 'mongodb://mongo-0.mongo:27017,mongo-1.mongo:27017,mongo-2.mongo:27017?replicaSet=rs0';

console.log(`Attempting to connect to ${uri}...`);
MongoClient.connect(uri, init);

function init(err, clientConn) {
    if (err) {
        console.log(`ERROR connecting to ${uri}`);
        console.log(err.message);
        return;
    }
    console.log(`Connected.`);

    const dbName = 'test';
    const db = clientConn.db(dbName);

    // check server config
    const config = db.serverConfig;
    console.log('db server config:');
    console.log(config);

    // Create a collection
    const newTableName = 'demographics';
    const newTableOptions = { };
    console.log(`Creating arbitrary ${newTableName} collection...`);
    db.createCollection(newTableName, newTableOptions).then((res) => {
        console.log(`Result of creation: ${res}`);

        // Run test
        return testDb(db, newTableName);
    }).then((res) => {
        console.log(`Test completed. Time diff: ${res}`);

        clientConn.close();
        return;
    }).catch((err) => {
        console.log(`ERROR bencharmking ${newTableName}: ${err.message}`);
        clientConn.close();
    });
}

function testDb(collection) {
    return new Promise((resolve, reject) => {
        let sampleData = parseCsvData(data);
        let numRecords = 1000;
        let hrTime = process.hrtime();
        let startTime = hrTime[0] * 1000000000 + hrTime[1]; // start in sE(-9)

        console.log(`Duplicating test data from ${sampleData.length} to ${numRecords} records...`);
        // add elements to array until it reaches target numRecords
        while (sampleData.length < numRecords) {
            let arbIndex = Math.floor(Math.random() * (sampleData.length - 1));
            sampleData.push(sampleData[arbIndex]);
        }

        console.log(`Test data reached ${sampleData.length}`);

        // Insert 1000 records
        collection.insertMany(sampleData, {}, (err, res) => {
            if (err) {
                console.log(`ERROR inserting into collection: ${err.message}`);
                return reject(err);
            }

            hrTime = process.hrtime();
            let endTime = hrTime[0] * 1000000000 + hrTime[1]; // end time in sE(-9)
            let nanoDiff = endTime - starTime;

            console.log(`Took ${nanoDiff} seconds to insert ${numRecords} records`);

            return resolve(nanoDiff);
        });
    });
}

module.exports.init = init;
module.exports.testDb = testDb;

