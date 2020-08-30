const axios = require('axios');
const crypto = require('crypto');
const { RecordType, FieldType } = require('./statics');
const { RequestOptions, DatahubOptions, Field } = require('./modules')

// 官方API参考文档 https://help.aliyun.com/document_detail/158856.html

/**
 * 构建规范头
 * @param {RequestOptions} reqOption 
 */
function buildCanonicalizedDataHubHeaders(reqOption) {
    let _canonicalizedHeaders = '';
    let _headersToSign = {};
    let _headers = reqOption.headers;
    for (const k in _headers) {
        // 1. 将所有以 x-datahub- 为前缀的HTTP请求头的名字转换成小写 。如X-DATAHUB-Client-Version:1.1需要转换成x-datahub-client-version:1.1。
        let _k = k.toLowerCase();
        if (_k.indexOf('x-datahub-') === 0) {
            _headersToSign[_k] = _headers[k];
        }
    }
    // console.debug('[DEBUG]', { _headersToSign });

    // 3. 将上一步得到的所有HTTP请求头按照名字的字典序进行升序排列。
    let _orderedKeys = Object.keys(_headersToSign).sort();
    // console.debug('[DEBUG]', { _orderedKeys });

    if (_orderedKeys.length > 0) {
        // 4. 删除请求头和内容之间分隔符两端出现的任何空格。如x-datahub-client-versionn : 1.1转换成：x-datahub-client-version:1.1。
        let _orderedHeadersToSign = _orderedKeys.map(
            (_item) => `${_item}:${_headersToSign[_item]}`
        );
        // console.debug('[DEBUG]', {
        //     _headersToSign_ordered: _orderedHeadersToSign,
        // });

        // 5. 将每一个头和内容用 \n 分隔符分隔拼成最后的CanonicalizedDataHubHeaders。
        _canonicalizedHeaders = _orderedHeadersToSign.join('\n');
    }

    return _canonicalizedHeaders;
}

/**
 * 构建规范资源
 * @param {RequestOptions} reqOption 
 */
function buildCanonicalizedResource(reqOption) {
    // 1. 将CanonicalizedResource置成空字符串 “”；
    let _canonicalizedResource = '';
    let _url = reqOption.url;
    // 2. 放入要访问的DataHub资源，如某个topic： /projects/test_project/topics/foo
    _canonicalizedResource += `${_url}`;

    // 3. 如果请求的资源包含额外的url参数，按照字典序，从小到大排列并以 & 为分隔符生成参数字符串。
    //    在CanonicalizedResource字符串尾添加 ？和参数字符串。
    //    此时的CanonicalizedResource如：/projects/test_project/topics/foo/connectors/sink_odps?donetime
    // TODO 暂时还未涉及到有参数的 API, 后续添加
    // let hasQuery = false; 
    // if (hasQuery) {
    //     let _orderedParams = '';
    //     _canonicalizedResource += `?${_orderedParams}`;
    // }

    // console.debug('[DEBUG]', { _canonicalizedResource });
    return _canonicalizedResource;
}

/**
 * 构建规范字符串
 * @param {String} reqOption 
 */
function buildCanonicalStr(reqOption) {
    let HTTPMethod = reqOption.method;
    let ContentType = reqOption.headers['Content-Type'];
    let date = reqOption.headers.Date;
    let canonicalizedDataHubHeaders = buildCanonicalizedDataHubHeaders(
        reqOption
    );
    let canonicalizedResource = buildCanonicalizedResource(reqOption);
    let _canonicalStr = `${HTTPMethod}\n${ContentType}\n${date}\n${canonicalizedDataHubHeaders}\n${canonicalizedResource}`;
    // console.debug('[DEBUG]', { _canonicalStr });
    return _canonicalStr;
}

/**
 * 生成签名
 * @param {RequestOptions} reqOption 
 * @param {String} accessKeySecret 
 */
function buildSignature(reqOption, accessKeySecret) {
    let canonicalStr = buildCanonicalStr(reqOption);
    let _sign = crypto
        .createHmac('sha1', accessKeySecret)
        .update(canonicalStr)
        .digest()
        .toString('base64');
    return _sign;
}

/**
 * 添加 Auth 头
 * @param {RequestOptions} reqOption 
 * @param {String} accessKeyId 
 * @param {String} accessKeySecret 
 */
function buildAuthorization(reqOption, accessKeyId, accessKeySecret) {
    if (!accessKeyId || !accessKeySecret) {
        throw new Error('Missing ACCESS_KEY_ID / ACCESS_KEY_SECRET');
    }
    let _sign = buildSignature(reqOption, accessKeySecret);
    // console.debug('[DEBUG]', { sign: _sign });
    reqOption.headers.Authorization = `DATAHUB ${accessKeyId}:${_sign}`;
    return reqOption;
}

/**
 * 执行 Restful 请求
 * @param {AxiosInstance} restClient 
 * @param {String} method 
 * @param {String} url 
 * @param {Object} data 
 */
async function _request(restClient, method, url, data) {
    let _reqOption = buildAuthorization(
        new RequestOptions(method, url, data),
        restClient.ACCESS_KEY_ID,
        restClient.ACCESS_KEY_SECRET)

    // console.debug('[DEBUG]', _reqOption)

    try {
        const response = await restClient(_reqOption);
        let {status, statusText, data} = response
        return {status, statusText, data}
    } catch (error) {
        if (error.response) {
            let {status, statusText, data} = error.response
            let {ErrorCode, ErrorMessage} = data
            throw new Error(`[${status} ${statusText}] ${ErrorCode}: ${ErrorMessage}`)
        } else {
            console.error('[ERROR]', error);
            throw error
        }
    }
}

class Datahub {
    /**
     * 
     * @param {DatahubOptions} options 
     */
    constructor(options) {
        this.restClient = axios.create({
            baseURL: options.ENDPOINT,
            timeout: 1000,
        });
        this.restClient.ACCESS_KEY_ID = options.ACCESS_KEY_ID;
        this.restClient.ACCESS_KEY_SECRET = options.ACCESS_KEY_SECRET;
    }


    // === Projects ===
    /**
     * 创建 Project
     * @param {String} projectName 
     * @param {String} comment 
     */
    createProject(projectName, comment = '') {
        return _request(this.restClient, 'POST', `/projects/${projectName}`, { Comment: comment });
    }

    /**
     * 查询 Project
     * @param {String} projectName 
     */
    getProject(projectName) {
        return _request(this.restClient, 'GET', `/projects/${projectName}`);
    }

    /**
     * 更新 Project
     * @param {String} projectName 
     * @param {String} comment 
     */
    updateProject(projectName, comment) {
        return _request(this.restClient, 'PUT', `/projects/${projectName}`, { Comment: comment });
    }

    /**
     * 删除 Project
     * @param {String} projectName 
     */
    deleteProject(projectName) {
        return _request(this.restClient, 'DELETE', `/projects/${projectName}`);
    }

    /**
     * 列出已有 Project 列表
     */
    listProjects() {
        return _request(this.restClient, 'GET', `/projects`);
    }

    // === Topic ===
    /**
     * 创建Topic
     * @param {String} projectName 
     * @param {String} topicName 
     * @param {Object} recordSchema  // TODO class RecordSchema
     * @param {String} comment 
     * @param {String} recordType 
     * @param {Number} shardCount 
     * @param {Number} lifeCycle 
     */
    createTopic(projectName, topicName, recordSchema, comment = '', recordType = RecordType.TUPLE, shardCount = 1, lifeCycle = 1) {
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}`, {
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
        return _request(this.restClient, 'GET', `/projects/${projectName}/topics/${topicName}`)
            .then(res => {
                // console.log(res.data);
                if(res.data && res.data.RecordSchema){
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
        return _request(this.restClient, 'PUT', `/projects/${projectName}/topics/${topicName}`, { Comment: comment })
    }

    /**
     * 删除 Topic
     * @param {String} projectName 
     * @param {String} topicName 
     */
    deleteTopic(projectName, topicName) {
        return _request(this.restClient, 'DELETE', `/projects/${projectName}/topics/${topicName}`)
    }

    /**
     * 查询 Topic 列表
     * @param {String} projectName 
     */
    listTopics(projectName) {
        return _request(this.restClient, 'GET', `/projects/${projectName}/topics`)
    }

    // === Shard ===
    /**
     * 获取Shard列表
     * @param {String} projectName 
     * @param {String} topicName 
     */
    listShard(projectName, topicName) {
        return _request(this.restClient, 'GET', `/projects/${projectName}/topics/${topicName}/shards`)
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
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/shards`, {
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
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/shards`, {
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
     * @param {Number} shardId 
     * @param {String} type // TODO class CursorType
     * @param {Number} sequence 
     * @param {Number} systemTime
     */
    getCursor(projectName, topicName, shardId = 0, type = 'OLDEST', sequence = null, systemTime = null) {
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/shards/${shardId}`, {
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
     * @param {String} shardId 
     * @param {Object} attributes 
     * @param {Array<String> | String} data 
     */
    // TODO !! class Record <- shardId, attributes, data  
    // TODO Datahub 服务端数据存储全部以字符串方式, 因此返回的 数据需要更具 schema 转换正确的数据格式 
    // pub(projectName, topicName, Records) {
    pub(projectName, topicName, shardId, attributes, data) {
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/shards`, {
            Action: 'pub',
            Records: [
                {
                    ShardId: shardId,
                    Attributes: attributes,
                    Data: data
                }
            ]
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
     * @param {Number} shardId 
     * @param {String} cursor 
     * @param {Number} limit 
     */
    sub(projectName, topicName, cursor, shardId = 0, limit = 10) {
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/shards/${shardId}`, {
            Action: 'sub',
            Cursor: cursor,
            Limit: limit
        })
    }
    getRecords(...args){
        return this.sub(...args)
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
     * @param {String} fieldType // TODO class FiledType
     */
    appendField(projectName, topicName, fieldName, fieldType) {
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}`, {
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
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`, {
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
        return _request(this.restClient, 'GET', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`)
    }

    /**
     * 查询 Connector 列表
     * GET /projects/<ProjectName>/topics/<TopicName>/connectors HTTP/1.1
     * @param {String} projectName 
     * @param {String} topicName 
     */
    listConnectors(projectName, topicName) {
        return _request(this.restClient, 'GET', `/projects/${projectName}/topics/${topicName}/connectors`)
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
        return _request(this.restClient, 'DELETE', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`)
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
            this.restClient,
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
    getConnectorStatus(projectName, topicName, connectorType, shardId) {
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`,
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
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/connectors/${connectorType}`,
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
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/subscriptions`,
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
        return _request(this.restClient, 'GET', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}`)

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
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/subscriptions`,
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
        return _request(this.restClient, 'DELETE', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}`)

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
        return _request(this.restClient, 'PUT', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}`, {
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
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}/offsets`, {
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
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}/offsets`, {
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
        return _request(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}/subscriptions/${subscriptionId}/offsets`, {
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


module.exports = Datahub;
