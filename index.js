'use strict';

const MongoClient = require('mongodb').MongoClient;
const SeparatorChunker = require('chunking-streams').SeparatorChunker;
const fs = require('fs');
const _clone = require('clone');
const _each = require('bluebird').Promise.each;


//////////////////////////////
//   Script Configuration   //
//////////////////////////////

const DATA_FILEPATH = './demographics.csv';
const URI = process.env.URI || 'mongodb://mongo-0.mongo:27017,mongo-1.mongo:27017?replicaSet=rs0';
const DB_NAME = 'test';
const TABLE_NAME = 'demographics';
const TABLE_OPTIONS = { autoIndexId: false, capped: true, size: 100000000 };
const TARGET_RECORD_QUANTITY = 5000;


/**
 * Returns an array of objects generated from the given CSV file's data,
 * where each objects' keys are the lowercase headers on the first line
 * of the CSV.
 *
 * @param {string} dataFilePath The filepath to the desired csv
 *
 * @return {Array} Containing objects as defined in the csv 
 */
function parseCsvData(dataFilePath) {
    let dataBlock = fs.readFileSync(dataFilePath);
    let dataArr = dataBlock.toString().split('\n');
    let headers = dataArr[0].toLowerCase().split(',');
    let fields = [];

    // Remove erroneous characters from headers
    for (let i = 0; i < headers.length; i++) {
        let refined = headers[i].replace(/[\W_]+/g,"_");

        fields.push(refined);
    }

    // Populate & format array of objects from CSV
    const formattedObjects = [];
    const ids = [];
    for(let i = 1; i < dataArr.length - 1; i++) {
        let data = dataArr[i].split(',');
        let obj = {};
        for(let j = 0; j < data.length; j++) {
            obj[fields[j].trim()] = data[j].trim();
        }

        // Add unique "_id" field from first header
        obj['_id'] = `${data[0].trim()}-${process.hrtime()[0]}`;
        // TODO: Remove debugging loop below (when applicable)
        // if (ids.indexOf(obj['_id']) < 0) {
        //     ids.push(obj['_id']);
        // } else {
        //     console.log(`ERROR pushing duplicate id: ${obj['_id']} at index ${i + 1}`);
        // }

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
        let sampleData = parseCsvData(DATA_FILEPATH);
        let numDuplicates = 0;
        let recordsInserted = 0;

        console.log(`Duplicating test data from ${sampleData.length} to ${numRecords} records...`);
        // Add elements to array until it reaches target numRecords
        while (sampleData.length < numRecords) {
            let arbIndex = Math.floor(Math.random() * (sampleData.length - 1));
            // TODO: fix cloning such that cloned objects dont share same ObjectId
            sampleData.push(_clone(sampleData[arbIndex]));
            numDuplicates += 1;
        }
        console.log(`Test data reached ${sampleData.length}. ${numDuplicates} records duped`);

        let _computeTimeDiff = () => {
            hrTime = process.hrtime();
            let endTime = hrTime[0] * 1000000000 + hrTime[1]; // end time in sE(-9)
            let nanoDiff = endTime - startTime;

            console.log(`Took ${nanoDiff} seconds to insert ${numRecords} records`);

            return resolve(nanoDiff);
        };

        let _insertCb = (err, res) => {
            if (err) {
                console.log(`ERR inserting into collection: ${err.message}`);
                return;
            }

            recordsInserted += 1;
            console.log(`records inserted: ${recordsInserted}`);

            if (recordsInserted >= numRecords) {
                _computeTimeDiff();
            }
        };

        // Insert 1000 records
        console.log(`Inserting documents into ${collection.collectionName}...`);
        let hrTime = process.hrtime();
        let startTime = hrTime[0] * 1000000000 + hrTime[1]; // start in sE(-9)

        // let waitTime = 100 + Math.ceil((numRecords * 2) / (i + 1));

        _each(sampleData, anObject => {
            return insertRecord(collection, anObject);
        }).then(res => {
            _computeTimeDiff();
        });

        // collection.insertMany(sampleData, {}, (err, res) => {
        //     if (err) {
        //         console.log(`ERROR inserting into collection: ${err.message}`);
        //         return reject(err);
        //     }

        //     hrTime = process.hrtime();
        //     let endTime = hrTime[0] * 1000000000 + hrTime[1]; // end time in sE(-9)
        //     let nanoDiff = endTime - startTime;

        //     console.log(`Took ${nanoDiff} seconds to insert ${numRecords} records`);

        //     return resolve(nanoDiff);
        // });
    });
}

/**
 * Returns an array of lowercase fields from a single string of comma-separated
 * words. Any spaces in the string will be converted to underscores.
 *
 * @param {String} str The string to parse fields from
 *
 * @return {Array} Containing a lowercased, underscored field for each element
 */
function parseFieldsFromString(str) {
    let fields = [];
    let headers = str.toLowerCase().split(',');

    for (let i = 0; i < headers.length; i++) {
        let refined = headers[i].trim().replace(/[\W_]+/g, '_');

        fields.push(refined);
    }

    return fields;
}

/**
 * Parses the given string for comma-separated values, and returns an object
 * whose keys are the given array of fields, and whose respective values
 * are determined based on their ordering in the comma-separated string.
 *
 * @param {String} str The single comma-separated string of values
 * @param {Array} fields The array of fields to become the object's keys
 *
 * @return {Object} The resulting parsed 1-level-deep object
 */
function parseObjectFromString(str, fields) {
    let numOfFields = fields.length;
    let values = str.split(',');
    let obj = { };

    for (let i = 0; i < values.length; i++) {
        if (i + 1 > numOfFields) {
            break;
        }
        obj[fields[i].trim()] = values[i].trim();
    }

    // Add unique "_id" field from first header
    obj['_id'] = values[0].trim();

    return obj;
}

/**
 * Inserted the given object into the specified collection. Promise returns
 * integer 1 if insertion was successful.
 *
 * @param {Collection} collection the MongoClient.Collection receiving records
 * @param {Object} objectToInsert The plain JS object to be inserted
 *
 * @return {Promise<int>} Truthy integer 1 indicating success
 */
function insertRecord(collection, objectToInsert) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            console.log(`Inserting record for zip: ${objectToInsert.jurisdiction_name}...`);
            collection.insert(objectToInsert, {}, (err, res) => {
                if (err) {
                    console.log(`ERROR inserting into collection: ${err.message}`);
                    // return reject(err);
                } else {
                    console.log(`Insert ${objectToInsert.jurisdiction_name} successful`);
                }

                return resolve(1);
            });
        }, 100);
    });
}

/**
 * Computes the time (in nanoseconds) to insert some target amount of
 * objects into the given Collection. Unlike timeRecordInsertion, this uses
 * a memory-efficient stream and inserts individual records one at a time.
 *
 * @param {Collection} collection The MongoClient.Collection receiving records
 * @param {int} numRecords The number of records to insert
 *
 * @return {Promise<int>} The time (in nanoseconds) to insert the target number
 *         of records.
 */
function timeStreamInsertion(collection, numRecords) {
    return new Promise((resolve, reject) => {
        let recordsInserted = 0;
        let fields = [];
        let dataStream = fs.createReadStream(DATA_FILEPATH);
        dataStream.setEncoding('utf8');
        let chunker = new SeparatorChunker();
        chunker.on('data', (chunk) => {
            setTimeout(() => {
            chunk = chunk.toString();

            // If string contains no numbers at all, must be header of csv
            if (!(/\d/.test(chunk))) {
                fields = parseFieldsFromString(chunk);
                console.log(`Parsed fields as: ${JSON.stringify(fields)}`);
            } else if (recordsInserted < numRecords) {
                let anObj = parseObjectFromString(chunk, fields);

                insertRecord(collection, anObj).then((res) => {
                    recordsInserted += res;
                    console.log(`Records inserted: ${recordsInserted}`);
                }).catch((err) => {
                    // do nothing
                });
            } else {
                hrTime = process.hrtime();
                let endTime = hrTime[0] * 1000000000 + hrTime[1]; // end time in sE(-9)
                let nanoDiff = endTime - startTime;

                return resolve(nanoDiff);
            }
            }, 100 + Math.ceil(numRecords / (recordsInserted + 1)));
        });

        let hrTime = process.hrtime();
        let startTime = hrTime[0] * 1000000000 + hrTime[1]; // start in sE(-9)
        // let isPaused = true;

        // console.log(`dataStream._readableState.ended?: ${dataStream._readableState.ended}`);

        // while (!dataStream._readableState.ended) {
        //     if (!isPaused) {
        //         console.log(`piped`);
        //         dataStream.unpipe(chunker);
        //         isPaused = true;
        //     } else {
        //         console.log(`unpiped`);
        //         isPaused = false;
        //         dataStream.pipe(chunker);
        //     }
        // }

        dataStream.pipe(chunker);
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

    // Create a collection
    console.log(`Creating arbitrary ${TABLE_NAME} collection...`);
    let collection = null;
    db.createCollection(TABLE_NAME, TABLE_OPTIONS).then((res) => {
        console.log(`Collection ${res.collectionName} created`);

        collection = res;
        // console.log(`Deleting items in collection ${collection.collectionName}...`);
        // return collection.deleteMany({});
        // }).then((res) => {
        // console.log(`Collection wiped. ${res.n} documents deleted`);

        // Run test
        return timeRecordInsertion(collection, TARGET_RECORD_QUANTITY);
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
module.exports.timeStreamInsertion = timeStreamInsertion;

