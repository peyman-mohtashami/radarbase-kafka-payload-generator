const { Buffer } = require('buffer');

// Example values
const keySchemaVersion = 1;
const valueSchemaVersion = 18;
const sourceId = 'afcc20b2-fd6a-4ec0-9333-cbc776fbd3a9';
const records = [{ time: 0, timeReceived: 1, x: 1, y: 2, z: 3 }];

// Simulate value encoder (should be replaced with actual encoder logic)
const valueEncoder = {
    encode: (record) => {
        const recordBuffer = Buffer.alloc(20); // Example buffer size
        // Encoding the record to the buffer (example)
        recordBuffer.writeInt32LE(record.time, 0);
        recordBuffer.writeInt32LE(record.timeReceived, 4);
        recordBuffer.writeInt32LE(record.x, 8);
        recordBuffer.writeInt32LE(record.y, 12);
        recordBuffer.writeInt32LE(record.z, 16);
        return recordBuffer;
    }
};

// Write records function
async function writeRecords() {
    const buffers = [];

    const startItem = () => {
        buffers.push(Buffer.from([0])); // Example start item (actual logic may vary)
    };

    const writeInt = (value) => {
        const intBuffer = Buffer.alloc(4);
        intBuffer.writeInt32LE(value);
        buffers.push(intBuffer);
    };

    const writeString = (value) => {
        const stringBuffer = Buffer.from(value, 'utf-8');
        buffers.push(stringBuffer);
    };

    const writeBytes = (buffer) => {
        buffers.push(buffer);
    };

    const writeArrayStart = () => {
        buffers.push(Buffer.from([1])); // Example array start (actual logic may vary)
    };

    const setItemCount = (count) => {
        const countBuffer = Buffer.alloc(8);
        countBuffer.writeBigInt64LE(BigInt(count));
        buffers.push(countBuffer);
    };

    const writeArrayEnd = () => {
        buffers.push(Buffer.from([2])); // Example array end (actual logic may vary)
    };

    const flush = () => {
        // Combine all buffers into one
        const combinedBuffer = Buffer.concat(buffers);
        console.log(combinedBuffer);
        return combinedBuffer;
    };

    startItem();
    writeInt(keySchemaVersion);
    writeInt(valueSchemaVersion);

    // Do not send project ID and user ID; encode with zeros
    writeInt(0);
    writeInt(0);
    writeString(sourceId);
    writeArrayStart();
    setItemCount(records.length);
    for (const record of records) {
        startItem();
        const temp = valueEncoder.encode(record);
        writeBytes(temp);
    }
    writeArrayEnd();
    return flush();
}

// Execute the function
writeRecords().then((resultBuffer) => {
    console.log('Encoded Buffer:', resultBuffer);
});
