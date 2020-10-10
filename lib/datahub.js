/**
 * Aliyun Datahub SDK for NodeJS
 */

const {
    DatahubOptions,
    Field,
    RecordSchema,
    RecordType,
    FieldType,
    TupleRecord,
    BlobRecord,
    CursorType,
    ConnectorType
} = require('./modules');
const Rest = require('./rest');

// 官方API参考文档 https://help.aliyun.com/document_detail/158856.html

class Datahub {
    /**
     * Datahub 客户端
     * @param {DatahubOptions} options
     * @param {Object} axiosOptions
     * @example
        const datahubOption = new DatahubOptions(
            'https://dh-cn-shanghai.aliyuncs.com',
            process.env.ACCESS_KEY_ID,
            process.env.ACCESS_KEY_SECRET)
        const dh = new Datahub(datahubOption, { timeout: 2000 })
     */
    constructor(options, axiosOptions) {
        this.rest = new Rest(options.ENDPOINT, options.ACCESS_KEY_ID, options.ACCESS_KEY_SECRET, axiosOptions)
    }


    // === Projects ===
    /**
     * 创建 Project
     * @param {String} projectName 
     * @param {String} comment
     * @example
        dh.createProject('my_project', 'my project')
     */
    createProject(projectName, comment = '') {
        return this.rest.request('POST', `/projects/${projectName}`, { Comment: comment });
    }

    /**
     * 查询 Project
     * @param {String} projectName 
     * @example
        dh.getProject('my_project', 'my project')
     */
    getProject(projectName) {
        return this.rest.request('GET', `/projects/${projectName}`);
    }

    /**
     * 更新 Project
     * @param {String} projectName 
     * @param {String} comment 
     * @example
        dh.updateProject('my_project', 'my awesome project')
    */
    updateProject(projectName, comment) {
        return this.rest.request('PUT', `/projects/${projectName}`, { Comment: comment });
    }

    /**
     * 删除 Project
     * @param {String} projectName
     * @example
        dh.deleteProject('my_project')
     */
    deleteProject(projectName) {
        return this.rest.request('DELETE', `/projects/${projectName}`);
    }

    /**
     * 列出已有 Project 列表
     * @example
        dh.listProjects()
     */
    listProjects() {
        return this.rest.request('GET', `/projects`);
    }

    // === Topic ===
    /**
     * 创建Topic
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {RecordSchema} recordSchema
     * @param {String} comment 
     * @param {String} recordType 
     * @param {Number} shardCount 
     * @param {Number} lifeCycle 
     * @example
        const schema = new RecordSchema([
            ['field_string', FieldType.STRING, true],
            ...
        ])
        dh.createTopic('my_project', 'my_topic', schema, 'my topic', RecordType.TUPLE, 1, 3 )
        dh.createTopic('my_project', 'my_topic', null, 'my topic', RecordType.BLOB, 1, 3 )
    */
    createTopic(projectName, topicName, recordSchema, comment = '', recordType = RecordType.TUPLE, shardCount = 1, lifeCycle = 3) {
        let _postData = {
            Action: 'create',
            ShardCount: shardCount,
            Lifecycle: lifeCycle,
            RecordType: recordType,
            Comment: comment
        }
        if (recordType === RecordType.TUPLE) _postData.RecordSchema = JSON.stringify(recordSchema)
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}`, _postData)
    }

    /**
     * 查询 Topic
     * @param {String} projectName 
     * @param {String} topicName 
     * @example
        dh.getTopic('my_project', 'my_topic')
     */
    getTopic(projectName, topicName) {
        return this.rest.request('GET', `/projects/${projectName}/topics/${topicName}`)
            .then(res => {
                // console.log(res.data);
                if (res.data && res.data.RecordSchema) {
                    let _schemaStr = res.data.RecordSchema
                    res.data.RecordSchema = JSON.parse(_schemaStr)
                }
                return res
            })
    }


    /**
     * 更新 Topic
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} comment 
     * @example
        dh.updateTopic('my_project', 'my_topic', 'my awesome topic')
     */
    updateTopic(projectName, topicName, comment) {
        return this.rest.request('PUT', `/projects/${projectName}/topics/${topicName}`, { Comment: comment })
    }

    /**
     * 删除 Topic
     * @param {String} projectName 
     * @param {String} topicName 
     * @example
        dh.deleteTopic('my_project', 'my_topic')
     */
    deleteTopic(projectName, topicName) {
        return this.rest.request('DELETE', `/projects/${projectName}/topics/${topicName}`)
    }

    /**
     * 查询 Topic 列表
     * @param {String} projectName
     * @example
        dh.listTopic('my_project')
     */
    listTopics(projectName) {
        return this.rest.request('GET', `/projects/${projectName}/topics`)
    }

    // === Shard ===
    /**
     * 获取Shard列表
     * @param {String} projectName 
     * @param {String} topicName
     * @example
        dh.updateTopic('my_project', 'my_topic')
     */
    listShard(projectName, topicName) {
        return this.rest.request('GET', `/projects/${projectName}/topics/${topicName}/shards`)
    }

    /**
     * 分裂Shard
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} shardId 
     * @param {String} splitKey 
     * @example
        dh.splitShard('my_project', 'my_topic', '0', '7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF')
    */
    splitShard(projectName, topicName, shardId, splitKey) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/shards`, {
            Action: 'split',
            ShardId: shardId,
            SplitKey: splitKey
        })
    }


    /**
     * 合并Shard
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} shardId 
     * @param {String} adjacentShardId 
     * @example
        dh.mergeShard('my_project', 'my_topic', '0', '1')
     */
    mergeShard(projectName, topicName, shardId, adjacentShardId) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/shards`, {
            Action: 'split',
            ShardId: shardId,
            AdjacentShardId: adjacentShardId
        })
    }

    /**
     * 查询数据Cursor
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} type
     * @param {String} shardId 
     * @param {Number} sequenceOrTimestamp
     * @example
        dh.getCursor('my_project', 'my_topic')
        dh.getCursor('my_project', 'my_topic', CursorType.OLDEST)
        dh.getCursor('my_project', 'my_topic', CursorType.LATEST, '0')
        dh.getCursor('my_project', 'my_topic', CursorType.SEQUENCE, '0', 1)
        dh.getCursor('my_project', 'my_topic', CursorType.SYSTEM_TIME, '0', 1599143251526000)
     */
    getCursor(projectName, topicName, type = CursorType.OLDEST, shardId = '0', sequenceOrTimestamp = null) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/shards/${shardId}`, {
            Action: 'cursor',
            Type: type,
            Sequence: type === CursorType.SEQUENCE ? sequenceOrTimestamp : null,
            SystemTime: type === CursorType.SYSTEM_TIME ? sequenceOrTimestamp : null
        })
    }

    /**
     * 写入数据 - 不按shard写入
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {TupleRecord | BlobRecord} records 
     * @param {Array<String> | String} data 
     * @example
        let testData = {
            field_string: 'abc',
            field_integer: 1,
            field_boolean: false,
            field_timestamp: new Date()
        }
        let testTupleRecords = []
        testTupleRecords.push(new TupleRecord(testData, testRecordSchema))
        dh.push('my_project', 'my_topic', testTupleRecords)
     */
    push(projectName, topicName, records) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/shards`, {
            Action: 'pub',
            Records: records
        })
    }

    /**
     * 读取数据 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {RecordSchema} recordSchema 
     * @param {String} cursor 
     * @param {String} shardId 
     * @param {Number} limit 
     * @example
        let recordSchema = (await dh.getTopic('my_project', 'my_topic')).data.RecordSchema
        let cursor = (await dh.getCursor('my_project', 'my_topic', ...)).data.Cursor
        dh.pull('my_project', 'my_topic', recordSchema , cursor, '0', 10)
     */
    pull(projectName, topicName, recordSchema, cursor, shardId = '0', limit = 10) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/shards/${shardId}`, {
            Action: 'sub',
            Cursor: cursor,
            Limit: limit
        }).then(res => {
            // 获取的数据都是字符串,如果传输 recordSchema 则将数据转换成正确的类型
            if (recordSchema) {
                res.data.Records.forEach(record => {
                    record.Data = TupleRecord.parse(record.Data, recordSchema)
                })
            }
            return res
        })
    }

    /**
     * 新增Field
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} fieldName 
     * @param {FieldType} fieldType
     * @example
        dh.appendField('my_project', 'my_topic', 'new_filed' , FieldType.STRING)
     */
    appendField(projectName, topicName, fieldName, fieldType) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}`, {
            Action: 'appendfield',
            FieldName: fieldName,
            FieldType: fieldType
        })
    }

    // ============== Connector 数据同步 ===============
    // TODO 需要开通相应的服务,所以暂时未做测试
    /**
     * 创建 Connector
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {ConnectorType} connectorType
     * @param {Array<String>} columnFields 
     * @param {Object} config 
     * @example
        let config = {
                "Project": "odpsProject",
                "Topic": "odpsTopic",
                "OdpsEndpoint": "xxx",
                "TunnelEndpoint": "xxx",
                "AccessId": "xxx",
                "AccessKey": "xxx",
                "PartitionMode": "SYSTEM_TIME",
                "TimeRange": 60,
                "PartitionConfig": {
                    "pt": "%Y%m%d",
                    "ct": "%H%M"
                }
            }
        let fields = ["field1", "field2"]
        dh.createConnector('my_project', 'my_topic', ConnectorType.SINK_ODPS, fields,  config)
     */
    createConnector(projectName, topicName, connectorType, columnFields, config) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`, {
            Type: connectorType,
            ColumnFields: columnFields,
            Config: config
        })
    }

    /**
     * 查询 Connector
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {ConnectorType} connectorType
     * @example
        dh.getConnector('my_project', 'my_topic', ConnectorType.SINK_ODPS)
     */
    getConnector(projectName, topicName, connectorType) {
        return this.rest.request('GET', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`)
    }

    /**
     * 查询 Connector 列表
     * @param {String} projectName 
     * @param {String} topicName 
     * @example
        dh.listConnectors('my_project', 'my_topic')
     */
    listConnectors(projectName, topicName) {
        return this.rest.request('GET', `/projects/${projectName}/topics/${topicName}/connectors`)
    }

    /**
     * 删除 Connector
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {ConnectorType} connectorType
     * @example
        dh.deleteConnector('my_project', 'my_topic', ConnectorType.SINK_ODPS)
     */
    deleteConnector(projectName, topicName, connectorType) {
        return this.rest.request('DELETE', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`)
    }

    /**
     * Reload Connector
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {ConnectorType} connectorType
     * @example
        dh.reloadConnector('my_project', 'my_topic', ConnectorType.SINK_ODPS)
     */
    reloadConnector(projectName, topicName, connectorType) {
        return _request(
            this.rest,
            'POST',
            `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`,
            {
                Action: 'reload'
            })
    }

    /**
     * 获取Connector Shard状态信息
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {ConnectorType} connectorType
     * @param {String} shardId 
     * @example
        dh.getConnectorStatus('my_project', 'my_topic', ConnectorType.SINK_ODPS, '0')
     */
    getConnectorStatus(projectName, topicName, connectorType, shardId = '0') {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`,
            {
                Action: 'status',
                ShardId: shardId
            })

    }

    /**
     * 追加 Connector Field
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {ConnectorType} connectorType
     * @param {String} fieldName
     * @example
        dh.getConnectorStatus('my_project', 'my_topic', ConnectorType.SINK_ODPS, 'new_field')
     */
    appendConnectorField(projectName, topicName, connectorType, fieldName) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`,
            {
                Action: 'appendfiled',
                FieldName: fieldName
            })
    }


    // ============ subscription 数据订阅: 用于服务端存储指针 ==============
    /**
     * 创建订阅
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} comment
     * @example
        dh.createSubscription('my_project', 'my_topic', 'my subscription')
     */
    createSubscription(projectName, topicName, comment) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/subscriptions`,
            {
                Action: 'create',
                Comment: comment
            })
    }

    /**
     * 查询订阅
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} subscriptionId
     * @example
        dh.createSubscription('my_project', 'my_topic', '1598602034559OYGWN')
     */
    getSubscription(projectName, topicName, subscriptionId) {
        return this.rest.request('GET', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}`)
    }

    /**
     * 查询订阅列表
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {Number} pageIndex 
     * @param {Number} pageSize
     * @example
        dh.listSubscription('my_project', 'my_topic', 1, 10)
     */
    listSubscription(projectName, topicName, pageIndex, pageSize) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/subscriptions`,
            {
                Action: 'list',
                PageIndex: pageIndex,
                PageSize: pageSize
            })
    }

    /**
     * 删除订阅
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} subscriptionId 
     * @example
        dh.listSubscription('my_project', 'my_topic', '1598602034559OYGWN')
     */
    deleteSubscription(projectName, topicName, subscriptionId) {
        return this.rest.request('DELETE', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}`)

    }

    /**
     * 更新订阅状态
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} subscriptionId 
     * @param {Number} state
     * @example
        dh.updateSubscription('my_project', 'my_topic', '1598602034559OYGWN', 1)
     */
    updateSubscription(projectName, topicName, subscriptionId, state) {
        return this.rest.request('PUT', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}`, {
            State: state
        })
    }


    /**
     * open 点位 session
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} subscriptionId 
     * @param {Array<String>} shardIds 
     * @example
        dh.openOffsetSession('my_project', 'my_topic', '1598602034559OYGWN', ['0'])
     * 
     */
    openOffsetSession(projectName, topicName, subscriptionId, shardIds) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}/offsets`, {
            Action: 'open',
            ShardIds: shardIds
        })
    }

    /**
     * 查询点位
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} subscriptionId 
     * @param {Array<String>} shardIds
     * @example
        dh.getOffset('my_project', 'my_topic', '1598602034559OYGWN', ['0'])
     */
    getOffset(projectName, topicName, subscriptionId, shardIds) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}/offsets`, {
            Action: 'get',
            ShardIds: shardIds
        })
    }

    /**
     * 提交点位
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} subscriptionId 
     * @param {Object} offsets
     * @example
     * let offsets = {
                0: {
                    Timestamp: 1000,
                    Sequence: 1,
                    Version: 1,
                    SessionId: 1
                }
            }
        dh.openOffsetSession('my_project', 'my_topic', '1598602034559OYGWN', offsets)
     */
    commitOffset(projectName, topicName, subscriptionId, offsets) {
        return this.rest.request('PUT', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}/offsets`, {
            Action: 'commit',
            Offsets: offsets
        })
    }


}

Datahub.RecordType = RecordType
Datahub.FieldType = FieldType
Datahub.DatahubOptions = DatahubOptions
Datahub.Field = Field
Datahub.RecordSchema = RecordSchema
Datahub.TupleRecord = TupleRecord
Datahub.BlobRecord = BlobRecord
Datahub.CursorType = CursorType
Datahub.ConnectorType = ConnectorType

module.exports = Datahub;
