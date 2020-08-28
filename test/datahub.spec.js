const Datahub = require("../lib/datahub");
const {RecordType} = Datahub

const dh = new Datahub({
    ENDPOINT: "https://dh-cn-shanghai.aliyuncs.com",
    ACCESS_KEY_ID: process.env.ACCESS_KEY_ID,
    ACCESS_KEY_SECRET: process.env.ACCESS_KEY_SECRET,
});

// console.log(dh);

// const dh2 = new Datahub({
//     ENDPOINT: "https://dh-cn-shanghai.aliyuncs.com",
//     ACCESS_KEY_ID: 'xxxx',
//     ACCESS_KEY_SECRET: 'xxxx' ,
// });

// dh.createProject('jk_test1','测试项目')
// dh.updateProject('jk_test1','测试项目更新描述')
// dh.getProject('jk_test1')
// dh.deleteProject('jk_test1')
// dh.listProjects()

let recordSchema = {
    fields:
        [{ name: 'field1', type: 'STRING' },
        { name: 'field2', type: 'BIGINT' }]
}

dh.createTopic('jk_test1', 'topic1', recordSchema, '测试 11', RecordType.TUPLE, 1, 3)
dh.createTopic('jk_test1', 'topic2', recordSchema)


// dh.getTopic('jk_test', 'foo');
