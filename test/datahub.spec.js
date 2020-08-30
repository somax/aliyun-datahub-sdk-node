const Datahub = require("../lib/datahub");
const { RecordType, FieldType, Options, Field } = Datahub

// const dh = new Datahub({
//     ENDPOINT: "https://dh-cn-shanghai.aliyuncs.com",
//     ACCESS_KEY_ID: process.env.ACCESS_KEY_ID,
//     ACCESS_KEY_SECRET: process.env.ACCESS_KEY_SECRET,
// });

const opt = new Options('https://dh-cn-shanghai.aliyuncs.com', process.env.ACCESS_KEY_ID, process.env.ACCESS_KEY_SECRET)
const dh = new Datahub(opt)

async function test() {
    let res;
    // console.log(dh);

    // const dh2 = new Datahub({
    //     ENDPOINT: "https://dh-cn-shanghai.aliyuncs.com",
    //     ACCESS_KEY_ID: 'xxxx',
    //     ACCESS_KEY_SECRET: 'xxxx' ,
    // });

    // res = await dh.deleteProject('jk_test1')
    // res = await dh.createProject('jk_test1','测试项目')
    // res = await dh.updateProject('jk_test1','测试项目更新描述111')
    // res = await dh.getProject('jk_test1')
    // res = await dh.listProjects()

    // res = await dh.listTopics('jk_test1')
    // res = await dh.getTopic('jk_test1', 'topic3')






    let fields = [
        new Field('field_string', FieldType.STRING),
        new Field('field_integer', FieldType.INTEGER),
        new Field('field_double', FieldType.DOUBLE),
        new Field('field_boolean', FieldType.BOOLEAN),
        new Field('field_timestamp', FieldType.TIMESTAMP),
        new Field('field_decimal', FieldType.DECIMAL),
    ]

    let recordSchema = {
        fields
        // [
        //     { name: 'field_string', type: 'STRING' },
        //     { name: 'field_bigint', type: 'BIGINT' },
        //     { name: 'field_double', type: 'Double' },
        //     { name: 'field_boolean', type: 'Boolean' , notnull: true},
        //     { name: 'field_timestamp', type: 'timestamp', notnull: false }
        // ]
    }

    console.log(recordSchema.fields);

    let data = {
        field_string: 'abc',
        field_integer: 1,
        field_double: 12.1,
        field_boolean: false,
        field_timestamp: 'Sun Aug 30 2020 22:03:35 GMT+0800 (GMT+08:00)',
        field_decimal: 123.5
    }
    let record = ['abc', '1', '12.1', 'false', String(Date.now()), '123.5']


    function _record2Data(record, schema) {
        let data = {}
        schema.fields.forEach((item, index) => {
            data[item.name] = FieldType.convertToJavaScriptType(record[index], item.type)
        });

        return data
    }

    function _data2Record(data, schema) {
        let record = []
        schema.fields.forEach((item, index) => {
            record[index] = FieldType.convertToDatahubType(data[item.name], item.type)
        });
        return record
    }

    console.log(_record2Data(record, recordSchema));
    console.log(_data2Record(data, recordSchema))
    // try {
    //     res = await dh.createTopic('jk_test1','topic3', recordSchema, 'test topic3', RecordType.TUPLE)
    // } catch (error) {
    //     console.log('xx----->>', error);
    // }

    // dh.createTopic('jk_test1','topic6', recordSchema, 'test topic', RecordType.TUPLE)
    // dh.createTopic('jk_test1','topic5', recordSchema)
    // dh.deleteTopic('jk_test1','topic3')
    // dh.updateTopic('jk_test1','topic2','update topic...')
    // dh.getTopic('jk_test1','topic2')
    // dh.getTopic('jk_test1','topic5')
    // dh.listTopics('jk_test1')
    // dh.getCursor('jk_test1','topic2', 0 , 'OLDEST')
    // dh.getCursor('jk_test1','topic2')

    // dh.getRecords('jk_test1', 'topic2', '30000000000000000000000000000000', 0)
    // dh.getRecords('jk_test1', 'topic2', '30000000000000000000000000000000')

    // // dh.pub('jk_test1','topic2','0', {attr1:'888'}, ['a','1'])



    // .then(res => console.log(JSON.stringify(res),res.data.RecordSchema))
    // .then(res => console.log(JSON.stringify(res),res.data))
    // .catch(err => console.error('xxxx',err))




    // dh.openOffsetSession()
}

test()