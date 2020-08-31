/**
 * 测试代码
 */
const Datahub = require("../lib/datahub");
const assert = require('assert');


const { RecordType, FieldType, Options, Field, RecordSchema, TupleRecord, BolbRecord } = Datahub

// const dh = new Datahub({
//     ENDPOINT: "https://dh-cn-shanghai.aliyuncs.com",
//     ACCESS_KEY_ID: process.env.ACCESS_KEY_ID,
//     ACCESS_KEY_SECRET: process.env.ACCESS_KEY_SECRET,
// });

const opt = new Options('https://dh-cn-shanghai.aliyuncs.com', process.env.ACCESS_KEY_ID, process.env.ACCESS_KEY_SECRET)
const dh = new Datahub(opt)

async function sleep(ms) {
    console.log('sleep', ms);
    return await new Promise(resolve => setTimeout(resolve, ms));
}

const spl = '\n---------------------------------\n\n'
async function log(method, ...args) {
    try {
        console.log(method, await dh[method](...args), spl);
    } catch (error) {
        console.error(method, error, spl)
    }
}

function test(expect, method, ...args) {
    return async () => {
        let res = await dh[method](...args);
        assert.equal(expect, res.statusText);
        console.log(JSON.stringify(res, null, 2));
        sleep(1000);
    }
}

async function prepare(method, ...args) {
    try { 
        await dh[method](...args);
        sleep(1000);
    } catch (error) { }

}

const testProjectName = 'jk_auto_test_project'
const testTopicName = 'auto_test_topic'


prepare('deleteProject', testProjectName)

describe('Project 操作测试', function () {
    it('创建', test('Created', 'createProject', testProjectName, 'test project'));
    it('更新', test('OK', 'updateProject', testProjectName, '测试要删除的项目'));
    it('查询', test('OK', 'getProject', testProjectName));
    it('列表', test('OK', 'listProjects', testProjectName));
    it('删除', test('OK', 'deleteProject', testProjectName));
});


prepare('createProject', testProjectName)

const testRecordSchema = new RecordSchema([
    ['field_string', FieldType.STRING, true],
    ['field_integer', FieldType.INTEGER, true],
    ['field_double', FieldType.DOUBLE, true],
    ['field_boolean', FieldType.BOOLEAN, true],
    ['field_decimal', FieldType.DECIMAL, true],
    ['field_timestamp', FieldType.TIMESTAMP, true],
])

describe('Topic 操作测试', function() {
    it('创建', test('Created', 'createTopic', testProjectName, testTopicName, testRecordSchema, 'test topic', RecordType.TUPLE));
    sleep(1000)
    it('更新', test('OK', 'updateTopic',testProjectName, testTopicName,'测试要删除的topic'));
    it('查询', test('OK', 'getTopic', testProjectName, testTopicName));
    it('列表', test('OK', 'listTopics', testProjectName, testTopicName));
    it('删除', test('OK', 'deleteTopic', testProjectName, testTopicName));
});


async function test1() {
    let res;
    // console.log(dh);

    // const dh2 = new Datahub({
    //     ENDPOINT: "https://dh-cn-shanghai.aliyuncs.com",
    //     ACCESS_KEY_ID: 'xxxx',
    //     ACCESS_KEY_SECRET: 'xxxx' ,
    // });

    console.log('============= start ============>');


    let recordSchema = new RecordSchema([
        ['field_string', FieldType.STRING, true],
        ['field_integer', FieldType.INTEGER, true],
        ['field_double', FieldType.DOUBLE, true],
        ['field_boolean', FieldType.BOOLEAN, true],
        ['field_decimal', FieldType.DECIMAL, true],
        ['field_timestamp', FieldType.TIMESTAMP, true],
    ])

    // console.log(recordSchema);

    let data = {
        field_string: 'abc',
        field_integer: 1,
        field_double: 12.1,
        field_boolean: false,
        field_timestamp: 'Sun Aug 30 2020 22:03:35 GMT+0800 (GMT+08:00)',
        // field_decimal: 123.5
    }
    // let record = ['abc', '1', '12.1', 'false', String(Date.now()), '123.5']
    // let record = ['abc', '1', '12.1', 'false', String(Date.now())]



    // console.log(_record2Data(record, recordSchema));
    // console.log(_data2Record(data, recordSchema))
    // try {
    //     res = await dh.createTopic('jk_test1','topic3', recordSchema, 'test topic3', RecordType.TUPLE)
    // } catch (error) {
    //     console.log('xx----->>', error);
    // }

    // dh.createTopic('jk_test1','topic6', recordSchema, 'test topic', RecordType.TUPLE)
    // dh.createTopic('jk_test1','topic8', recordSchema)
    // dh.deleteTopic('jk_test1','topic3')
    // dh.updateTopic('jk_test1','topic2','update topic...')
    // dh.getTopic('jk_test1','topic2')
    // dh.getTopic('jk_test1','topic5')
    // dh.listTopics('jk_test1')
    // dh.getCursor('jk_test1','topic2', 0 , 'OLDEST')
    // dh.getCursor('jk_test1','topic2')


    // dh.pull('jk_test1', 'topic5', recordSchema, '30000000000000000000000000000000')
    // .then(res => console.log(JSON.stringify(res),res.data.Records))
    // .catch(err => console.error('xxxx',err))

    // let records = []
    // records.push(new TupleRecord(data, recordSchema))
    // dh.push('jk_test1','topic5', records)
    // // .then(res => console.log(JSON.stringify(res),res.data.RecordSchema))
    // .then(res => console.log(JSON.stringify(res)))
    // .catch(err => console.error('xxxx',err))

    // dh.openOffsetSession()


    console.log('\n<============= end =============');

}

// test()