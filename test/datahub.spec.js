/**
 * 测试代码
 */
const Datahub = require("../lib/datahub");
const assert = require('assert');


const { RecordType, FieldType, DatahubOptions, Field, RecordSchema, TupleRecord, BlobRecord, CursorType } = Datahub

// const dh = new Datahub({
//     ENDPOINT: "https://dh-cn-shanghai.aliyuncs.com",
//     ACCESS_KEY_ID: process.env.ACCESS_KEY_ID,
//     ACCESS_KEY_SECRET: process.env.ACCESS_KEY_SECRET,
// });

const datahubOption = new DatahubOptions('https://dh-cn-shanghai.aliyuncs.com', process.env.ACCESS_KEY_ID, process.env.ACCESS_KEY_SECRET)
const dh = new Datahub(datahubOption)


const testProjectName = 'auto_test_project'
const testTopicName = 'auto_test_topic'
const testRecordSchema = new RecordSchema([
    ['field_string', FieldType.STRING, true],
    ['field_integer', FieldType.INTEGER, true],
    ['field_double', FieldType.DOUBLE, true],
    ['field_boolean', FieldType.BOOLEAN, true],
    ['field_decimal', FieldType.DECIMAL, true],
    ['field_timestamp', FieldType.TIMESTAMP, true],
])
let testData = {
    field_string: 'abc',
    field_integer: 1,
    field_double: 12.1,
    field_boolean: false,
    field_decimal: 123.5,
    field_timestamp: 'Sun Aug 30 2020 22:03:35 GMT+0800 (GMT+08:00)'
}

let testTupleRecords = []
testTupleRecords.push(new TupleRecord(testData, testRecordSchema))



describe('Modules 单元测试', function () {
    describe('Record', function () {
        it('new BlobRecord', () => {
            assert.deepEqual({
                Attributes: {foo:"bar"},
                Data: 'dGVzdA==',
                ShardId: '8'
            }, new BlobRecord('test', {foo:"bar"}, '8'))
        })

        it('BlobRecord.base64Decode', () => {
            let testStr = 'test'
            let testBuffer = Buffer.from(testStr)
            let blobRecord = new BlobRecord(testStr)
            assert.deepEqual(testBuffer, BlobRecord.base64Decode(blobRecord.Data))
        })
    })
})


describe('Datahub 功能测试', function () {

    describe('Project/Topic 操作测试', async function () {

        // it('createProject', async () => {
        //     assert.strictEqual('Created',
        //         (await dh.createProject(testProjectName, '测试创建 project')).statusText
        //     )
        // })
        // it('updateProject', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.updateProject(testProjectName, '测试更新 project')).statusText
        //     )
        // })
        // it('getProject', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.getProject(testProjectName)).statusText
        //     )
        // })
        // it('listProjects', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.listProjects(testProjectName)).statusText
        //     )
        // })

        // it('createTopic-tuple', async () => {
        //     assert.strictEqual('Created',
        //         (await dh.createTopic(testProjectName, testTopicName,  testRecordSchema, '测试创建 Tuple topic', RecordType.TUPLE)).statusText
        //     )
        // })

        // it('createTopic-blob', async () => {
        //     assert.strictEqual('Created',
        //         (await dh.createTopic(testProjectName, testTopicName, null, '测试创建 Blob topic', RecordType.BLOB)).statusText
        //     )
        // })




        // it('updateTopic', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.updateTopic(testProjectName, testTopicName, '测试更新描述')).statusText
        //     )
        // })

        // it('getTopic', async () => {
        //     let res = await dh.getTopic(testProjectName, testTopicName)
        //     assert.strictEqual('OK', res.statusText)
        //     console.log(res.data);
        // })

        // it('push-tuple', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.push(testProjectName, testTopicName, testRecords)).statusText
        //     )
        // })

        // it('push-blob', async () => {
        //     let _testBlobRecords = [new BlobRecord('test')]
        //     assert.strictEqual('OK',
        //         (await dh.push(testProjectName, testTopicName, _testBlobRecords)).statusText
        //     )
        // })
        
        // it('getCursor-default', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.getCursor(testProjectName, testTopicName)).statusText
        //     )
        // })
        // it('getCursor-oldest', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.getCursor(testProjectName, testTopicName, CursorType.OLDEST)).statusText
        //     )
        // })
        // it('getCursor-latest', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.getCursor(testProjectName, testTopicName, CursorType.LATEST)).statusText
        //     )
        // })
        // it('getCursor-sequence', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.getCursor(testProjectName, testTopicName, CursorType.SEQUENCE, '0', 0)).statusText
        //     )
        // })
        // it('getCursor-system_time', async () => {
        //     let timestamp = new Date('2020-08-28').getTime()
        //     assert.strictEqual('OK',
        //         (await dh.getCursor(testProjectName, testTopicName, CursorType.SYSTEM_TIME, '0', timestamp)).statusText
        //     )
        // })

 
        // it('pull-tuple', async () => {
        //     let cursor  = (await dh.getCursor(testProjectName, testTopicName)).data.Cursor
        //     let res = await dh.pull(testProjectName, testTopicName, testRecordSchema, cursor)
        //     assert.strictEqual('OK', res.statusText);
        //     console.log(res.data);
        // })
        it('pull-blob', async () => {
            let cursor  = (await dh.getCursor(testProjectName, testTopicName)).data.Cursor
            let res = await dh.pull(testProjectName, testTopicName, null, cursor)
            assert.strictEqual('OK', res.statusText);
            console.log(res.data);
        })


        // it('appendField', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.appendField(testProjectName, testTopicName, 'field_append', FieldType.STRING)).statusText
        //     )
        // })
        // it('listTopics', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.listTopics(testProjectName, testTopicName)).statusText
        //     )
        // })
        // it('deleteTopic', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.deleteTopic(testProjectName, testTopicName)).statusText
        //     )
        // })


        // it('deleteProject', async () => {
        //     assert.strictEqual('OK',
        //         (await dh.deleteProject(testProjectName)).statusText
        //     )
        // })
    });

})
