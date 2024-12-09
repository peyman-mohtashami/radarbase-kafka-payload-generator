const avro = require('avro-js');
const { Buffer } = require('buffer');

function bytesToHexString(bytes) {
    return bytes.map(byte => ('00' + (byte & 0xFF).toString(16)).slice(-2)).join(' ');
}

const ambientLightSchemaStr = `
{"type":"record",
    "name":"SensorKitAmbientLight",
    "namespace":"org.radarcns.passive.apple.sensorkit",
    "doc":"Data describes the amount of ambient light in the user’s environment.",
    "fields":[
    {"name":"time","type":"double","doc":"Device timestamp in UTC (s)."},
    {"name":"timeReceived","type":"double","doc":"Device receiver timestamp in UTC (s)."},
    {"name":"device","type":"string","doc":"Device model."},
    {"name":"chromaticityX","type":"float","doc":"x-value of the coordinate pair that describes the light brightness and tint."},
    {"name":"chromaticityY","type":"float","doc":"y-value of the coordinate pair that describes the light brightness and tint."},
    {"name":"lux","type":"float","doc":"Illuminance (lx)."},
    {"name":"placement",
        "type":{"type":"enum","name":"SensorPlacement","doc":"Directional values that describe light-source location with respect to the sensor.",
            "symbols":["FRONT_BOTTOM","FRONT_BOTTOM_LEFT","FRONT_BOTTOM_RIGHT","FRONT_LEFT","FRONT_RIGHT","FRONT_TOP","FRONT_TOP_LEFT","FRONT_TOP_RIGHT","UNKNOWN"]
        },
        "doc":"The light’s location relative to the sensor.","default":"UNKNOWN"}
]}`;

// const ambientLightJsonStr = `{"time": 0.1, "timeReceived": 0.2, "device": "iPhone","chromaticityX": 0.123,"chromaticityY":0.456,"lux":0.789,"placement":"FRONT_TOP"}`;
const ambientLightJsonStr = `{
    "time": 1719844860.000,
    "timeReceived": 1719844860.000,
    "device": "UNKNOWN",
    "chromaticityX": 0.123,
    "chromaticityY": 0.124,
    "lux": 0.125,
    "placement": "FRONT_BOTTOM"
    }`

// Define the PhoneLight schema
const lightSchemaStr = `{
  "namespace": "org.radarcns.passive.phone",
  "type": "record",
  "name": "PhoneLight",
  "doc": "Data from the light sensor in luminous flux per unit area.",
  "fields": [
    { "name": "time", "type": "double", "doc": "Device timestamp in UTC (s)." },
    { "name": "timeReceived", "type": "double", "doc": "Device receiver timestamp in UTC (s)." },
    { "name": "light", "type": "float", "doc": "Illuminance (lx)." }
  ]
}`;
const lightJsonStr = '{"time":0.1,"timeReceived":0.2,"light":0.3}';

const accelerometerSchemaStr = '{\n' +
    '  "namespace": "org.radarcns.passive.phone",\n' +
    '  "type": "record",\n' +
    '  "name": "PhoneAcceleration",\n' +
    '  "doc": "Data from 3-axis accelerometer sensor with gravitational constant g as unit.",\n' +
    '  "fields": [\n' +
    '    { "name": "time", "type": "double", "doc": "Device timestamp in UTC (s)." },\n' +
    '    { "name": "timeReceived", "type": "double", "doc": "Device receiver timestamp in UTC (s)." },\n' +
    '    { "name": "x", "type": "float", "doc": "Acceleration in the x-axis (g)." },\n' +
    '    { "name": "y", "type": "float", "doc": "Acceleration in the y-axis (g)." },\n' +
    '    { "name": "z", "type": "float", "doc": "Acceleration in the z-axis (g)." }\n' +
    '  ]\n' +
    '}'
// const accelerometerJsonStr = '{"time":0,"timeReceived":1,"x":1,"y":2,"z":3}';
const accelerometerJsonStr1 = '{"time":1719844860000,"timeReceived":1719844860000,"x":0.1,"y":0.2,"z":0.3}';
const accelerometerJsonStr2 = '{"time":1719851923000,"timeReceived":1719851923000,"x":0.4,"y":0.5,"z":0.6}';
const accelerometerJsonStr3 = '{"time":1719851924000,"timeReceived":1719851924000,"x":0.7,"y":0.8,"z":0.9}';

const batterySchemaStr = '{\n' +
    '  "namespace": "org.radarcns.passive.phone",\n' +
    '  "type": "record",\n' +
    '  "name": "PhoneBatteryLevel",\n' +
    '  "doc": "Phone battery level.",\n' +
    '  "fields": [\n' +
    '    {"name": "time", "type": "double", "doc": "Device timestamp in UTC (s)."},\n' +
    '    {"name": "timeReceived", "type": "double", "doc": "Device receiver timestamp in UTC (s)."},\n' +
    '    {"name": "batteryLevel", "type": "float", "doc": "Battery level from 0 to 1."},\n' +
    '    {"name": "isPlugged", "type": "boolean", "doc": "Whether the phone is connected to a power source."},\n' +
    '    {"name": "status", "type": {\n' +
    '      "name": "BatteryStatus",\n' +
    '      "type": "enum",\n' +
    '      "doc": "Android device battery states.",\n' +
    '      "symbols": ["CHARGING", "DISCHARGING", "NOT_CHARGING", "FULL", "UNKNOWN"]\n' +
    '      }, "doc": "Android battery states.", "default": "UNKNOWN"}\n' +
    '  ]\n' +
    '}'

const batteryJsonStr = '{"time":1719844860000,"timeReceived":1719844860000,"batteryLevel":1,"isPlugged":false,"status":"NOT_CHARGING"}';

const serverSchemaStr = '{\n' +
    '  "namespace": "org.radarcns.monitor.application",\n' +
    '  "type": "record",\n' +
    '  "name": "ApplicationServerStatus",\n' +
    '  "doc": "Server connection status with android client.",\n' +
    '  "fields": [\n' +
    '    { "name": "time", "type": "double", "doc": "Device timestamp in UTC (s)." },\n' +
    '    { "name": "serverStatus", "type": {\n' +
    '      "name": "ServerStatus",\n' +
    '      "type": "enum",\n' +
    '      "doc": "Server connection status.",\n' +
    '      "symbols": ["CONNECTED", "DISCONNECTED", "UNKNOWN"]}, "doc": "Application server connection status.", "default": "UNKNOWN" },\n' +
    '    { "name": "ipAddress", "type": ["null", "string"], "doc": "Hardware identifier of client application.", "default": null }\n' +
    '  ]\n' +
    '}'

// Define the RecordSet schema
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

// Function to encode JSON to Avro buffer
function encodeToAvroBuffer(json, schemaStr) {
    const schema = avro.parse(schemaStr);
    return schema.toBuffer(JSON.parse(json));
}

// // Encode the PhoneLight record
const lightAvroBuffer = encodeToAvroBuffer(lightJsonStr, lightSchemaStr);
const accAvroBuffer1 = encodeToAvroBuffer(accelerometerJsonStr1, accelerometerSchemaStr);
const accAvroBuffer2 = encodeToAvroBuffer(accelerometerJsonStr2, accelerometerSchemaStr);
const accAvroBuffer3 = encodeToAvroBuffer(accelerometerJsonStr3, accelerometerSchemaStr);
const batteryAvroBuffer = encodeToAvroBuffer(batteryJsonStr, batterySchemaStr);
const ambientLightAvroBuffer = encodeToAvroBuffer(ambientLightJsonStr, ambientLightSchemaStr);
console.log(bytesToHexString(Array.from(ambientLightAvroBuffer)))

// Create the RecordSet object directly
const recordSetJsonObj = {
    keySchemaVersion: 1,
    valueSchemaVersion: 1,
    sourceId: "687ca31d-5a9c-4660-81e4-8bd3aa2712ea",
    data: [ambientLightAvroBuffer, ambientLightAvroBuffer,ambientLightAvroBuffer,ambientLightAvroBuffer,ambientLightAvroBuffer,ambientLightAvroBuffer,ambientLightAvroBuffer,ambientLightAvroBuffer,ambientLightAvroBuffer,ambientLightAvroBuffer], //accAvroBuffer1, accAvroBuffer2, accAvroBuffer3], //accAvroBuffer] //lightAvroBuffer] // Use Buffer directly
};

// Encode the RecordSet record
const recordSetSchema = avro.parse(recordSetSchemaStr);
const recordSetAvroBuffer = recordSetSchema.toBuffer(recordSetJsonObj);

console.log("0---")
console.log(Array.from(recordSetAvroBuffer));
console.log("1---")

console.log()
console.log("2---")
console.log(bytesToHexString(Array.from(recordSetAvroBuffer)))
console.log("3---")

const fs = require('fs');

// Uint8Array
// const data = new Uint8Array(Buffer.from('Hello Node.js'));
// fs.writeFile('message.txt', data, callback);

// Buffer
fs.writeFile('output1000.bin', Buffer.from(Array.from(recordSetAvroBuffer)), (err) => {
    if (err) throw err;
    console.log('It\'s saved!');
});

// string
// fs.writeFile('message.txt', 'Hello Node.js', callback);

// var callback = (err) => {
//     if (err) throw err;
//     console.log('It\'s saved!');
// }

/*// Example hex string array
const hexArray = ['020200004864373631623030662d616161612d343537332d613464612d66396166313636303038643802145c8f2ae693a0d941000000'];

// Step 1: Join the hex array into a single hex string
const hexString = hexArray.join('');

// Step 2: Convert hex string to a Buffer
const buffer = Buffer.from(hexString, 'hex');

// Step 3: Convert Buffer to a UTF-8 encoded string
const text = buffer.toString('utf8');

console.log(text); // Output: "Hello World!"
*/

/*
const zlib = require('zlib');

// Example hex string array (for this example, it's a single string)
const hexArray = [
    "020200004864373631623030662d616161612d343537332d613464612d66396166313636303038643802145c8f2ae693a0d941000000"
];

// Step 1: Join the hex array into a single hex string
const hexString = hexArray.join('');

// Step 2: Convert hex string to a Buffer
const buffer = Buffer.from(hexString, 'hex');

// Step 3: Unzip the Buffer
zlib.gunzip(buffer, (err, unzippedBuffer) => {
    if (err) {
        console.error('An error occurred while unzipping:', err);
        return;
    }

    // Step 4: Convert Buffer to a UTF-8 encoded string
    const text = unzippedBuffer.toString('utf8');
    console.log(text); // Output the unzipped and converted text
});
*/
