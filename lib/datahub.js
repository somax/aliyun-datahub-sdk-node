/**
 * Datahub 客户端
 * MaXiaojun<somaxj@163.com>@2020
 */

const {
    DatahubOptions,
    Field,
    RecordSchema,
    RecordType,
    FieldType,
    TupleRecord,
    BlobRecord
} = require('./modules');
const Rest = require('./rest');

// 官方API参考文档 https://help.aliyun.com/document_detail/158856.html

class Datahub {
    /**
     * Datahub 客户端
     * @param {DatahubOptions} options 
     */
    constructor(options) {
        this.rest = new Rest(options.ENDPOINT, options.ACCESS_KEY_ID, options.ACCESS_KEY_SECRET)
    }


    // === Projects ===
    /**
     * 创建 Project
     * @param {String} projectName 
     * @param {String} comment 
     */
    createProject(projectName, comment = '') {
        return this.rest.request('POST', `/projects/${projectName}`, { Comment: comment });
    }

    /**
     * 查询 Project
     * @param {String} projectName 
     */
    getProject(projectName) {
        return this.rest.request('GET', `/projects/${projectName}`);
    }

    /**
     * 更新 Project
     * @param {String} projectName 
     * @param {String} comment 
     */
    updateProject(projectName, comment) {
        return this.rest.request('PUT', `/projects/${projectName}`, { Comment: comment });
    }

    /**
     * 删除 Project
     * @param {String} projectName 
     */
    deleteProject(projectName) {
        return this.rest.request('DELETE', `/projects/${projectName}`);
    }

    /**
     * 列出已有 Project 列表
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
     */
    createTopic(projectName, topicName, recordSchema, comment = '', recordType = RecordType.TUPLE, shardCount = 1, lifeCycle = 1) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}`, {
            Action: 'create',
            ShardCount: shardCount,
            Lifecycle: lifeCycle,
            RecordType: recordType,
            RecordSchema: JSON.stringify(recordSchema),
            Comment: comment
        })
    }

    /**
     * 查询 Topic
     * @param {String} projectName 
     * @param {String} topicName 
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
     */
    updateTopic(projectName, topicName, comment) {
        return this.rest.request('PUT', `/projects/${projectName}/topics/${topicName}`, { Comment: comment })
    }

    /**
     * 删除 Topic
     * @param {String} projectName 
     * @param {String} topicName 
     */
    deleteTopic(projectName, topicName) {
        return this.rest.request('DELETE', `/projects/${projectName}/topics/${topicName}`)
    }

    /**
     * 查询 Topic 列表
     * @param {String} projectName 
     */
    listTopics(projectName) {
        return this.rest.request('GET', `/projects/${projectName}/topics`)
    }

    // === Shard ===
    /**
     * 获取Shard列表
     * @param {String} projectName 
     * @param {String} topicName 
     */
    listShard(projectName, topicName) {
        return this.rest.request('GET', `/projects/${projectName}/topics/${topicName}/shards`)
    }

    /**
     * 分裂Shard
     * @example
     * POST /projects/<ProjectName>/topics/<TopicName>/shards HTTP/1.1
     * {
            Action: 'split',
            ShardId: '0',
            SplitKey: '7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'
        }
    * @param {String} projectName 
    * @param {String} topicName 
    * @param {String} shardId 
    * @param {String} splitKey 
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
     * @example
     * POST /projects/<ProjectName>/topics/<TopicName>/shards HTTP/1.1
     * {
            Action: 'merge',
            ShardId: '0',
            AdjacentShardId: '1'
        }
    */
    /**
     * 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} shardId 
     * @param {String} adjacentShardId 
     */
    mergeShard(projectName, topicName, shardId, adjacentShardId) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/shards`, {
            Action: 'split',
            ShardId: shardId,
            AdjacentShardId: adjacentShardId
        })
    }
    // _do('POST','/projects/jk_test/topics/foo/shards', {
    //     Action: 'merge',
    //     ShardId: '1',
    //     AdjacentShardId: '2'
    //     })

    /**
     * 查询数据Cursor
     * @example
     * POST /projects/<ProjectName>/topics/<TopicName>/shards/<ShardId> HTTP/1.1
     * {
        Action: 'cursor',
        Type: 'SEQUENCE', // OLDEST, LATEST, SYSTEM_TIME, SEQUENCE
        SystemTime: 1598516937840, // Type为SYSTEM_TIME时填写，单位Ms
        Sequence: 4 // Type为SEQUENCE时填写
    }
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} shardId 
     * @param {String} type // TODO class CursorType
     * @param {Number} sequence 
     * @param {Number} systemTime
     */
    getCursor(projectName, topicName, shardId = '0', type = 'OLDEST', sequence = null, systemTime = null) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/shards/${shardId}`, {
            Action: 'cursor',
            Type: type,
            Sequence: sequence,
            SystemTime: systemTime
        })
    }

    /**
     * 写入数据 - 不按shard写入
     * @example
     * POST /projects/<ProjectName>/topics/<TopicName>/shards HTTP/1.1
     * {
            Action: 'pub',
            Records: [
                {
                    ShardId: '0',
                    Attributes: {
                        attr1: 'value1',
                        attr2: 'value2'
                    },
                    Data: ['A','B','3','4'] 
                    //  Data: 'Base64String' // BLOB
                }
            ]
        }
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {TupleRecord | BlobRecord} records  //TODO handle blob record
     * @param {Array<String> | String} data 
     */
    push(projectName, topicName, records) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/shards`, {
            Action: 'pub',
            Records: records
        })
    }

    /**
     * 读取数据
     * @example
     * POST /projects/<ProjectName>/topics/<TopicName>/shards/<ShardId> HTTP/1.1
     * {
            Action: 'sub',
            Cursor: '30005af19b3800000000000000000000',
            Limit: 1
        }
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {RecordSchema} recordSchema 
     * @param {String} cursor 
     * @param {String} shardId 
     * @param {Number} limit 
     */
    pull(projectName, topicName, recordSchema, cursor, shardId = '0', limit = 10) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/shards/${shardId}`, {
            Action: 'sub',
            Cursor: cursor,
            Limit: limit
        }).then(res => {
            // console.log({res});
            res.data.Records.forEach( record => {
                record.Data = TupleRecord.parse(record.Data, recordSchema) //TODO handle both tuple and blob record
            })
            return res
        })
    }

    /**
     * 新增Field
     * @example
     * POST /projects/<ProjectName>/topics/<TopicName> HTTP/1.1
     * {
            Action: 'appendfield',
            FieldName: 'field1',
            FieldType: 'BIGINT'
        }
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} fieldName 
     * @param {FieldType} fieldType // TODO class FiledType
     */
    appendField(projectName, topicName, fieldName, fieldType) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}`, {
            Action: 'appendfield',
            FieldName: fieldName,
            FieldType: fieldType
        })
    }

    // ============== Connector 数据同步 ===============
    /**
     * 创建 Connector
     * @example
     * POST /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
     * {
            "Type": "SINK_ODPS",
            "ColumnFields": ["field1", "field2"],
            "Config": {
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
        }
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} connectorType //TODO class ConnectorType
     * @param {Array<String>} columnFields 
     * @param {Object} config 
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
     * @example
     * GET /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} connectorType //TODO class ConnectorType
     */
    getConnector(projectName, topicName, connectorType) {
        return this.rest.request('GET', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`)
    }

    /**
     * 查询 Connector 列表
     * GET /projects/<ProjectName>/topics/<TopicName>/connectors HTTP/1.1
     * @param {String} projectName 
     * @param {String} topicName 
     */
    listConnectors(projectName, topicName) {
        return this.rest.request('GET', `/projects/${projectName}/topics/${topicName}/connectors`)
    }

    /**
     * 删除 Connector
     * DELETE /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
     */
    /**
     * 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} connectorType  //TODO class ConnectorType
     */
    deleteConnector(projectName, topicName, connectorType) {
        return this.rest.request('DELETE', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`)
    }

    /**
     * Reload Connector
     * POST /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
     * {
            Action: 'reload'
        }
    */
    /**
     * 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} connectorType  //TODO class ConnectorType
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
     * POST /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
     * {
            Action: 'status',
            ShardId: '0'
        }
    */
    /**
     * 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} connectorType  //TODO class ConnectorType
     * @param {String} shardId 
     */
    getConnectorStatus(projectName, topicName, connectorType, shardId = '0') {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`,
            {
                Action: 'status',
                ShardId: shardId
            })

    }

    /**
     * Append Connector Field
     * POST /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
        {
            Action: 'appendfiled',
            FieldName: 'field1'
        }
     */
    /**
     * 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} connectorType  //TODO class ConnectorType
     * @param {String} fieldName 
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
     * POST /projects/<ProjectName>/topics/<TopicName>/subscriptions HTTP/1.1
     * {
            Action: 'create',
            Comment: 'xxxx'
        }
    */
    /**
     * 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} comment 
     */
    createSubscription(projectName, topicName, comment) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/subscriptions`,
            {
                Action: 'create',
                Comment: comment
            })
    }
    // _do('POST','/projects/jk_test/topics/foo/subscriptions',
    // {
    //     Action: 'create',
    //     Comment: 'create from api'
    // })

    /**
     * 查询订阅
     * GET /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubscriptionId> HTTP/1.1
     */
    // _do('GET','/projects/jk_test/topics/foo/subscriptions/1598602034559OYGWN')
    /**
     * 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} subscriptionId 
     */
    getSubscription(projectName, topicName, subscriptionId) {
        return this.rest.request('GET', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}`)

    }

    /**
     * 查询订阅列表
     * POST /projects/<ProjectName>/topics/<TopicName>/subscriptions HTTP/1.1
     * {
            Action: 'list',
            PageIndex: 1,
            PageSize: 10
        }
    */
    /**
     * 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {Number} pageIndex 
     * @param {Number} pageSize 
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
     * DELETE /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubscriptionId> HTTP/1.1
     */
    /**
     * 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} subscriptionId 
     */
    deleteSubscription(projectName, topicName, subscriptionId) {
        return this.rest.request('DELETE', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}`)

    }

    /**
     * 更新订阅状态
     * PUT /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubscriptionId> HTTP/1.1
     */
    // _do('PUT','/projects/jk_test/topics/foo/subscriptions/1598602034559OYGWN',{
    //     State: 1
    // })
    /**
     * 
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {String} subscriptionId 
     * @param {Number} state 
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
     */
    openOffsetSession(projectName, topicName, subscriptionId, shardIds) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}/offsets`, {
            Action: 'open',
            ShardIds: shardIds
        })
    }
    // _do('POST','/projects/jk_test/topics/foo/subscriptions/1598602034559OYGWN/offsets',{
    //     Action: 'open',
    //     ShardIds: ['0']
    // })

    /**
     * 查询点位
     * POST /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubscriptionId>/offsets HTTP/1.1
     * {
            Action: 'get',
            ShardIds: ['0']
        }
    */
    getOffset(projectName, topicName, subscriptionId, shardIds) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}/offsets`, {
            Action: 'get',
            ShardIds: shardIds
        })
    }
    // _do('POST','/projects/jk_test/topics/foo/subscriptions/1598602034559OYGWN/offsets',{
    //     Action: 'get',
    //     ShardIds: ['0']
    // })

    /**
     * 提交点位
     * PUT /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubscriptionId>/offsets HTTP/1.1
     * {
            Action: 'commit',
            Offsets: {
                0: {
                    Timestamp: 1000,
                    Sequence: 1,
                    Version: 1,
                    SessionId: 1
                }
            }
        }
    */
    commitOffset(projectName, topicName, subscriptionId, offsets) {
        return this.rest.request('POST', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}/offsets`, {
            Action: 'commit',
            Offsets: offsets
        })
    }
    // _do('PUT','/projects/jk_test/topics/foo/subscriptions/1598602034559OYGWN/offsets',{
    //     Action: 'commit',
    //     Offsets: {
    //         0: {
    //             Timestamp: 100000000000,
    //             Sequence: 2,
    //             Version: 0,
    //             SessionId: 1
    //         }
    //     }
    // })

}

Datahub.RecordType = RecordType
Datahub.FieldType = FieldType
Datahub.Options = DatahubOptions
Datahub.Field = Field
Datahub.RecordSchema = RecordSchema
Datahub.TupleRecord = TupleRecord
Datahub.BlobRecord = BlobRecord


module.exports = Datahub;
