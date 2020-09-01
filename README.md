# Datahub NodeJS Client

## 阿里云 Datahub Restful API 的 NodeJS 封装

> 官网没有提供 NodeJS 版本的 SDK, 所以基于 Restful 接口文档自己封装了一下, 方便开发使用. 

**项目尚在开发中...**

## Usage

```sh
yarn add aliyun-datahub-sdk
```

```js
const Datahub = require("../lib/datahub");
const { RecordType, FieldType, DatahubOptions, Field, 
        RecordSchema, TupleRecord, BlobRecord, CursorType } = Datahub;


// 创建 Datahub Client
const datahubOption = new DatahubOptions(
    "https://dh-cn-shanghai.aliyuncs.com",
    process.env.ACCESS_KEY_ID,
    process.env.ACCESS_KEY_SECRET
);
const dh = new Datahub(datahubOption);

// 创建 Project
dh.createProject('new_project', '测试创建 project')

// 创建 Topic
const testRecordSchema = new RecordSchema([
    //[ name, type, notnull ]   
    ['field_string', FieldType.STRING, true],
    ['field_integer', FieldType.INTEGER, false],
    ['field_boolean', FieldType.BOOLEAN, true],
    ['field_timestamp', FieldType.TIMESTAMP, true]
])
dh.createTopic('new_project', 'new_tuple_topic', testRecordSchema, '测试创建 Tuple topic', RecordType.TUPLE))

dh.createTopic('new_project', 'new_blob_topic', null, '测试创建 Blob topic', RecordType.BLOB))

// 推送数据
let testData = {
    field_string: 'abc',
    field_integer: 1,
    field_boolean: false,
    field_timestamp: new Date()
}
let testTupleRecords = []
testTupleRecords.push(new TupleRecord(testData, testRecordSchema))
dh.push('new_project', 'new_tuple_topic', testTupleRecords))

let testBlobRecords = [new BlobRecord('test')]
dh.push('new_project', 'new_blob_topic', testBlobRecords))

// 拉取数据(从头开始)
let _schema = (await dh.getTopic('new_project', 'new_tuple_topic')).data.RecordSchema
let _cursor = (await dh.getCursor('new_project', 'new_tuple_topic')).data.Cursor
let res = await dh.pull('new_project', 'new_tuple_topic', _schema, _cursor)
_cursor = res.NextCursor
let records = res.Records
// ...更新 cursor 以循环获取数据

// 如果是 blob 类型, schema 参数设置为空
dh.pull('new_project', 'new_blob_topic', null, _firstCursor)



```


## Test

```js
yarn test
```