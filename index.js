const avro = require('avro-js');
const { Buffer } = require('buffer');
const fs = require('fs');

const NUMBER_OF_RECORDS = 50000;
const PROJECT_ID = "test";
const USER_ID = "6a4c27e4-652b-4131-8154-e5bfd6209894";
const SOURCE_ID = "583d253b-eca2-4c95-ab59-a3c5e90ce06a";
const KEY_SCHEMA_ID = 3;
const KEY_SCHEMA_VALUE = 7;

function bytesToHexString(bytes) {
    return bytes.map(byte => ('00' + (byte & 0xFF).toString(16)).slice(-2)).join(' ');
}

const schemaStr = `{
    "type":"record",
    "name":"PhoneAcceleration",
    "namespace":"org.radarcns.passive.phone",
    "doc":"Data from 3-axis accelerometer sensor with gravitational constant g as unit.",
    "fields":[
        {"name":"time","type":"double","doc":"Device timestamp in UTC (s)."},
        {"name":"timeReceived","type":"double","doc":"Device receiver timestamp in UTC (s)."},
        {"name":"x","type":"float","doc":"Acceleration in the x-axis (g)."},
        {"name":"y","type":"float","doc":"Acceleration in the y-axis (g)."},
        {"name":"z","type":"float","doc":"Acceleration in the z-axis (g)."}
    ]
}`

const records = []
for (let i = 0; i< NUMBER_OF_RECORDS; i++) {
    records.push({
        "value": {"time": 1728976800 + i, "timeReceived": 1728976800 + i, "x": i / 100000, "y": i / 100000, "z": i / 100000},
        "key": {
            "projectId": {"string": PROJECT_ID},
            "userId": USER_ID,
            "sourceId": SOURCE_ID
        }
    })
}

const payload = {
    "key_schema_id": KEY_SCHEMA_ID,
    "value_schema_id": KEY_SCHEMA_VALUE,
    "records": records
}
const payloadString = JSON.stringify(payload, null, 2);

fs.writeFile('./output/acceleration_payload_1.json', payloadString, (err) => {
    if (err) {
        console.error('Error writing json file', err);
    } else {
        console.log('Successfully wrote json file');
    }
});

const recordSetSchemaStr = `{
  "namespace": "org.radarcns.kafka",
  "name": "RecordSet",
  "type": "record",
  "doc": "Abbreviated record set, meant for binary data transfers of larger sets of data. It can contain just a source ID and the record values. The record keys are deduced from authentication parameters. This method of data transfer requires that the data actually adheres to the schemas identified by the schema version.",
  "fields": [
    {"name": "keySchemaVersion", "type": "int", "doc": "Key schema version for the given topic."},
    {"name": "valueSchemaVersion", "type": "int", "doc": "Value schema version for the given topic."},
    {"name": "projectId", "type": ["null", "string"], "doc": "Project ID of the sent data. If null, it is attempted to be deduced from the credentials.", "default": null},
    {"name": "userId", "type": ["null", "string"], "doc": "User ID of the sent data. If null, it is attempted to be deduced from the credentials.", "default": null},
    {"name": "sourceId", "type": "string", "doc": "Source ID of the sent data."},
    {"name": "data", "type": {"type": "array", "items": "bytes", "doc": "Binary serialized Avro records."}, "doc": "Collected data. This should just contain the value records."}
  ]
}`;

function encodeToAvroBuffer(json, schemaStr) {
    const schema = avro.parse(schemaStr);
    return schema.toBuffer(JSON.parse(json));
}

const data = records.map(r => {
    const value = r.value
    return encodeToAvroBuffer(JSON.stringify(value), schemaStr)
})

const recordSetJsonObj = {
    keySchemaVersion: 1,
    valueSchemaVersion: 1,
    sourceId: SOURCE_ID,
    data: data
};

const recordSetSchema = avro.parse(recordSetSchemaStr);
const recordSetAvroBuffer = recordSetSchema.toBuffer(recordSetJsonObj);

fs.writeFile('./output/acceleration_binary_1.bin', Buffer.from(Array.from(recordSetAvroBuffer)), (err) => {
    if (err) {
        console.error('Error writing binary file', err);
    } else {
        console.log('Successfully wrote binary file');
    }
});

