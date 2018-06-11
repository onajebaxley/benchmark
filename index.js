'use strict';

const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const _clone = require('clone');
const data = fs.readFileSync('./demographics.csv');


//////////////////////////////
//   Script Configuration   //
//////////////////////////////

// const URI = 'mongodb://mongo-0.mongo:27017,mongo-1.mongo:27017,mongo-2.mongo:27017?replicaSet=rs0';
const URI = 'mongodb://mongo-0.mongo:27017';
const DB_NAME = 'test';
const TABLE_NAME = 'demographics';
const TABLE_OPTIONS = { autoIndexId: false, indexOptionDefaults: { } };
const TARGET_RECORD_QUANTIY = 1000;


/**
 * Returns an array of objects generated from the given CSV data,
 * where each objects' keys are the lowercase headers on the first line
 * of the CSV.
 *
 * @param {string} dataFile The data read from some csv
 *
 * @return {Array} Containing objects defined in the csv 
 */
function parseCsvData(dataFile) {
    let dataArr = dataFile.toString().split('\n');
    let headers = dataArr[0].toLowerCase().split(',');
    let fields = [];

    // Remove erroneous characters from headers
    for (let i = 0; i < headers.length; i++) {
        let refined = headers[i].replace(/[\W_]+/g,"_");

        fields.push(refined);
    }

    // Populate & format array of objects from CSV
    const formattedObjects = [];
    for(let i = 1; i < dataArr.length - 1; i++) {
        let data = dataArr[i].split(',');
        let obj = {};
        for(let j = 0; j < data.length; j++) {
            obj[fields[j].trim()] = data[j].trim();
        }

        // Add unique "_id" field from first header
        obj['_id'] = data[0].trim();

        formattedObjects.push(obj);
    }

    return formattedObjects;
}

/**
 * Computes the time (in nanoseconds) to insert some target amount of
 * objects into the given Collection.
 *
 * @param {Collection} collection The MongoClient.Collection receiving records
 * @param {int} numRecords The number of records to insert
 *
 * @return {Promise<int>} The time (in nanoseconds) to insert the target number
 *         of records.
 */
function timeRecordInsertion(collection, numRecords) {
    return new Promise((resolve, reject) => {
        let sampleData = parseCsvData(data);
        let initSampleLength = sampleData.length;

        console.log(`Duplicating test data from ${sampleData.length} to ${numRecords} records...`);
        // Add elements to array until it reaches target numRecords
        while (sampleData.length < numRecords) {
            let arbIndex = Math.floor(Math.random() * (sampleData.length - 1));
            // TODO: fix cloning such that cloned objects dont share same ObjectId
            sampleData.push(_clone(sampleData[arbIndex]));
        }
        console.log(`Test data reached ${sampleData.length}. ${numRecords - initSampleLength} records duped`);

        // Insert 1000 records
        console.log(`Inserting documents into ${collection.collectionName}...`);
        let hrTime = process.hrtime();
        let startTime = hrTime[0] * 1000000000 + hrTime[1]; // start in sE(-9)

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


/**
 * Entrypoint: creates the desired table, then performs benchmark and logs
 * output.
 *
 * @param {MongoError} err The error to be populated if client connection fails
 * @param {MongoClient} clientConn The connected client
 */
function init(err, clientConn) {
    if (err) {
        console.log(`ERROR connecting to ${URI}`);
        console.log(err.message);
        return;
    }
    console.log(`Connected.`);

    const db = clientConn.db(DB_NAME);

    // Check server config
    // TODO: delete extraneous config log below
    const config = db.serverConfig;
    console.log('db server config:');
    console.log(config);

    // Create a collection
    console.log(`Creating arbitrary ${TABLE_NAME} collection...`);
    db.createCollection(TABLE_NAME, TABLE_OPTIONS).then((res) => {
        console.log(`Collection ${res.collectionName} created`);

        // Run test
        return timeRecordInsertion(res, TARGET_RECORD_QUANTIY);
    }).then((res) => {
        console.log(`Insertion(s) complete. Time diff: ${res}`);

        clientConn.close();
        return;
    }).catch((err) => {
        console.log(`ERROR bencharmking ${TABLE_NAME}: ${err.message}`);
        clientConn.close();
    });
}

console.log(`Attempting to connect to ${URI}...`);
MongoClient.connect(URI, init);


module.exports.init = init;
module.exports.timeRecordInsertion = timeRecordInsertion;

