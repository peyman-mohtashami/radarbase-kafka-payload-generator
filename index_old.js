const avro = require('avro-js');
const { Buffer } = require('buffer');

function index(json, schemaStr) {
    // Parse the schema
    const schema = avro.parse(schemaStr);

    // Convert the JSON to an Avro buffer
    const buf = schema.toBuffer(JSON.parse(json));

    // Return the buffer as a byte array
    return Array.from(buf);
}
/*
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
const accelerometerJsonStr = '{"time":0,"timeReceived":1,"x":1,"y":2,"z":3}';
const accelerometerByteArray = Buffer.from(accelerometerJsonStr);
console.log(accelerometerByteArray);

const accelerometerAvroBytes = index(accelerometerJsonStr, accelerometerSchemaStr);
console.log(accelerometerAvroBytes);
const accelerometerBufferData = Buffer.from(accelerometerAvroBytes);
console.log(accelerometerBufferData.toString())
*/
const lightSchemaStr = '{\n' +
    '  "namespace": "org.radarcns.passive.phone",\n' +
    '  "type": "record",\n' +
    '  "name": "PhoneLight",\n' +
    '  "doc": "Data from the light sensor in luminous flux per unit area.",\n' +
    '  "fields": [\n' +
    '    { "name": "time", "type": "double", "doc": "Device timestamp in UTC (s)." },\n' +
    '    { "name": "timeReceived", "type": "double", "doc": "Device receiver timestamp in UTC (s)." },\n' +
    '    { "name": "light", "type": "float", "doc": "Illuminance (lx)." }\n' +
    '  ]\n' +
    '}'
const lightJsonStr = '{"time":0.1,"timeReceived":0.2,"light":0.3}';
const lightByteArray = Buffer.from(lightJsonStr);
// console.log(lightByteArray);

const lightAvroBytes = index(lightJsonStr, lightSchemaStr);
// console.log(lightAvroBytes);
const lightBufferData = Buffer.from(lightAvroBytes);
console.log(lightBufferData)
// console.log(lightBufferData.toString())
const byteArray = Array.from(lightBufferData);


const recordSetSchemaStr =  '{\n' +
    '    "namespace": "org.radarcns.kafka",\n' +
    '    "name": "RecordSet",\n' +
    '    "type": "record",\n' +
    '    "doc": "Abbreviated record set, meant for binary data transfers of larger sets of data. It can contain just a source ID and the record values. The record keys are deduced from authentication parameters. This method of data transfer requires that the data actually adheres to the schemas identified by the schema version.",\n' +
    '    "fields": [\n' +
    '        {"name": "keySchemaVersion", "type": "int", "doc": "Key schema version for the given topic."},\n' +
    '        {"name": "valueSchemaVersion", "type": "int", "doc": "Value schema version for the given topic."},\n' +
    '        {"name": "projectId", "type": ["null", "string"], "doc": "Project ID of the sent data. If null, it is attempted to be deduced from the credentials.", "default": null},\n' +
    '        {"name": "userId", "type": ["null", "string"], "doc": "User ID of the sent data. If null, it is attempted to be deduced from the credentials.", "default": null},\n' +
    '        {"name": "sourceId", "type": "string", "doc": "Source ID of the sent data."},\n' +
    '        {"name": "data", "type": {"type": "array", "items": "bytes", "doc": "Binary serialized Avro records."}, "doc": "Collected data. This should just contain the value records."}\n' +
    '    ]\n' +
    '}';
const recordSetJsonObj = {
    "keySchemaVersion": 1,
    "valueSchemaVersion": 18,
    "sourceId":"afcc20b2-fd6a-4ec0-9333-cbc776fbd3a9",
    "data":[byteArray]
};
// const recordSetJsonStr = '{' +
//     '"keySchemaVersion":1,' +
//     '"valueSchemaVersion":18,' +
//     '"sourceId":"afcc20b2-fd6a-4ec0-9333-cbc776fbd3a9",' +
//     '"data":['+lightBufferData.toString()+']' +
//     '}';
const recordSetJsonStr = JSON.stringify(recordSetJsonObj)
const recordSetByteArray = Buffer.from(recordSetJsonStr);
// const recordSetByteArray = Buffer.from(recordSetJsonStr);
// console.log(recordSetByteArray);

const recordSetAvroBytes = index(recordSetJsonStr, recordSetSchemaStr);
// console.log(recordSetAvroBytes);
const recordSetBufferData = Buffer.from(recordSetAvroBytes);

console.log(recordSetBufferData)
// console.log(recordSetBufferData.toString())
// const recordSetSchemaJson1 = '{' +
//     '"key_schema_id":"",' +
//     '"value_schema_id":"",' +
//     '"records":"[{' +
//     '"keySchemaVersion":1,' +
//     '"valueSchemaVersion":18,' +
//     '"sourceId":"afcc20b2-fd6a-4ec0-9333-cbc776fbd3a9",' +
//     '"data":[]' +
//     '}]"' +
//     '}';



// const recordSetBytes = index(recordSetSchemaJson, recordSetSchema);
// console.log('Class: index, Function: , Line 100 ' , );
// console.log(recordSetBytes);
// console.log('Class: index, Function: , Line 102 ' , );

const fs = require('fs');

// Uint8Array
// const data = new Uint8Array(Buffer.from('Hello Node.js'));
// fs.writeFile('message.txt', data, callback);

// Buffer
fs.writeFile('output1', Buffer.from(recordSetBufferData), (err) => {
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
