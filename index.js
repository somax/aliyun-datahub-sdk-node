/**
 * 阿里云 Datahub Restful API 调用示例
 * by MaXiaojun<somaxj@163.com>
 */
const axios = require("axios");
const crypto = require("crypto");

const ENDPOINT = "https://dh-cn-shanghai.aliyuncs.com";
const ACCESS_KEY_ID = process.env.ACCESS_KEY_ID || "";
const ACCESS_KEY_SECRET = process.env.ACCESS_KEY_SECRET || "";

// 官方API参考文档 https://help.aliyun.com/document_detail/158856.html

function buildCanonicalizedDataHubHeaders(reqOption) {
    let _canonicalizedHeaders = "";
    let _headersToSign = {};
    let _headers = reqOption.headers;
    for (const k in _headers) {
        // 1. 将所有以 x-datahub- 为前缀的HTTP请求头的名字转换成小写 。如X-DATAHUB-Client-Version:1.1需要转换成x-datahub-client-version:1.1。
        let _k = k.toLowerCase();
        if (_k.indexOf("x-datahub-") === 0) {
            _headersToSign[_k] = _headers[k];
        }
    }
    console.debug('[DEBUG]',{ _headersToSign });

    // 3. 将上一步得到的所有HTTP请求头按照名字的字典序进行升序排列。
    let _orderedKeys = Object.keys(_headersToSign).sort();
    console.debug('[DEBUG]',{ _orderedKeys });

    if (_orderedKeys.length > 0) {
        // 4. 删除请求头和内容之间分隔符两端出现的任何空格。如x-datahub-client-versionn : 1.1转换成：x-datahub-client-version:1.1。
        let _orderedHeadersToSign = _orderedKeys.map(
            (_item) => `${_item}:${_headersToSign[_item]}`
        );
        console.debug('[DEBUG]',{ _headersToSign_ordered: _orderedHeadersToSign });

        // 5. 将每一个头和内容用 \n 分隔符分隔拼成最后的CanonicalizedDataHubHeaders。
        _canonicalizedHeaders = _orderedHeadersToSign.join("\n");
    }

    return _canonicalizedHeaders;
}

function buildCanonicalizedResource(reqOption) {
    // 1. 将CanonicalizedResource置成空字符串 “”；
    let _canonicalizedResource = "";
    let _url = reqOption.url;
    // 2. 放入要访问的DataHub资源，如某个topic： /projects/test_project/topics/foo
    _canonicalizedResource += `${_url}`;
    // 3. 如果请求的资源包含额外的url参数，按照字典序，从小到大排列并以 & 为分隔符生成参数字符串。
    //    在CanonicalizedResource字符串尾添加 ？和参数字符串。
    //    此时的CanonicalizedResource如：/projects/test_project/topics/foo/connectors/sink_odps?donetime
    let hasQuery = false //TODO 
    if (hasQuery) {
        let _orderedParams = "";
        _canonicalizedResource += `?${_orderedParams}`;
    }

    console.log({_canonicalizedResource});
    return _canonicalizedResource;
}

function buildCanonicalStr(reqOption) {

    let HTTPMethod = reqOption.method;
    let ContentType = reqOption.headers["Content-Type"];
    let date = reqOption.headers.Date;
    let canonicalizedDataHubHeaders = buildCanonicalizedDataHubHeaders(reqOption);
    let canonicalizedResource = buildCanonicalizedResource(reqOption);
    let _canonicalStr = `${HTTPMethod}\n${ContentType}\n${date}\n${canonicalizedDataHubHeaders}\n${canonicalizedResource}`;
    console.log({_canonicalStr});
    return _canonicalStr;
}

/**
 * 生成签名
 */
function buildSignature(reqOption) {
    let canonicalStr = buildCanonicalStr(reqOption);
    let _sign = crypto
        .createHmac("sha1", ACCESS_KEY_SECRET)
        .update(canonicalStr)
        .digest()
        .toString("base64");
    return _sign;
}

/**
 * 添加 Authorization
 * @param {Object} reqOption
 */
function buildAuthorization(reqOption) {
    if( !ACCESS_KEY_ID || !ACCESS_KEY_SECRET){
        throw new Error('Missing ACCESS_KEY_ID/ACCESS_KEY_SECRET')
    }
    let _sign = buildSignature(reqOption);
    console.debug('[DEBUG]',{ sign: _sign });
    reqOption.headers.Authorization = `DATAHUB ${ACCESS_KEY_ID}:${_sign}`;
    return reqOption;
}

const datahubClient = axios.create({
    baseURL: ENDPOINT,
    timeout: 1000
});


// 测试 获取项目清单
async function _do(method, url, data) {
    let _option = {
        method,
        url,
        headers: {
            // "Content-Type": "application/json; charset=utf-8",
            "Content-Type": "application/json;",
            Date: new Date().toGMTString(),
            "x-datahub-client-version": "1.1"
        },
        data
    };

    _reqOption = buildAuthorization(_option);

    console.debug('[DEBUG]',_reqOption);

    try {
        const response = await datahubClient(_reqOption);
        console.log("[INFO]", response.data);
    } catch (error) {
        if (error.response) {
            console.error(
                "[ERROR]",
                error.response.status,
                error.response.statusText,
                error.response.data
            );
        } else {
            console.error(error);
        }
    }
}

/**
 * 创建 Project
 * 1.只能包含字母，数字和下划线(_)
 * 2.必须以字母或下划线(_)开头
 * 3.名称长度限制在1-32个字节之间
 * POST /projects/<ProjectName> HTTP/1.1
 */
// _do('POST','/projects/jk_test', { Comment: '新建测试项目'});

/**
 * 查询Project
 * GET /projects/<ProjectName> HTTP/1.1
 */
// _do('GET', '/projects/jk_test')


/**
 * 列出已有 Project 列表
 * GET /projects HTTP/1.1
 */
// _do('GET','/projects');

/**
 * 更新 Project
 * PUT /projects/<ProjectName> HTTP/1.1
 */
// _do('PUT', '/projects/jk_test', { Comment: '测试更新描述'})


/**
 * 删除Project
 * DELETE /projects/<ProjectName> HTTP/1.1
 */
// _do('DELETE', '/projects/jk_test')

/**
 * 创建Topic
 * POST /projects/<ProjectName>/topics/<TopicName> HTTP/1.1
 * {
        "Action": "create",
        "ShardCount": 1,
        "Lifecycle": 1,
        "RecordType": "TUPLE",
        "RecordSchema": "{\"fields\":[{\"name\":\"field1\",\"type\":\"STRING\"},{\"name\":\"field2\",\"type\":\"BIGINT\"}]}",
        "Comment": "create topic"
    }
*/
// _do('POST',
//     '/projects/jk_test/topics/foo',
//     {
//         Action: 'create',
//         ShardCount: 1,
//         Lifecycle: 1,
//         RecordType: 'TUPLE',
//         RecordSchema: '{"fields":[{"name":"field1","type":"STRING"},{"name":"field2","type":"BIGINT"}]}',
//         Comment: '测试 topic'
//     })

    
/**
 * 查询Topic
 * GET /projects/<ProjectName>/topics/<TopicName> HTTP/1.1
 */
// _do('GET','/projects/jk_test/topics/foo')

/**
 * 查询Topic列表
 * GET /projects/<ProjectName>/topics HTTP/1.1
 */
// _do('GET','/projects/jk_test/topics')


/**
 * 更新Topic
 * PUT /projects/<ProjectName>/topics/<TopicName> HTTP/1.1
 * {
        "Comment": "update comment"
    }
 */
// _do('PUT','/projects/jk_test/topics/foo',{Comment: "更新测试描述"})


/**
 * 删除Topic
 * DELETE /projects/<ProjectName>/topics/<TopicName> HTTP/1.1
 */
// _do('DELETE','/projects/jk_test/topics/foo')

/**
 * 获取Shard列表
 * GET /projects/<ProjectName>/topics/<TopicName>/shards HTTP/1.1
 */
// _do('GET','/projects/jk_test/topics/foo/shards')

/**
 * 分裂Shard
 * POST /projects/<ProjectName>/topics/<TopicName>/shards HTTP/1.1
 * {
        "Action": "split",
        "ShardId": "0",
        "SplitKey": "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
    }
 */
// _do('POST','/projects/jk_test/topics/foo/shards', {
//     Action: "split",
//     ShardId: "0",
//     SplitKey: "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
//     })


/**
 * 合并Shard
 * POST /projects/<ProjectName>/topics/<TopicName>/shards HTTP/1.1
 * {
        "Action": "merge",
        "ShardId": "0",
        "AdjacentShardId": "1"
    }
 */
// _do('POST','/projects/jk_test/topics/foo/shards', {
//     Action: "merge",
//     ShardId: "1",
//     AdjacentShardId: "2"
//     })

/**
 * 查询数据Cursor
 * POST /projects/<ProjectName>/topics/<TopicName>/shards/<ShardId> HTTP/1.1
 * {
    "Action": "cursor",
    "Type": "SEQUENCE", // OLDEST, LATEST, SYSTEM_TIME, SEQUENCE
    "Sequence": 1
    SystemTime: 1598516937840, // Type为SYSTEM_TIME时填写，单位Ms
    Sequence: 4 // Type为SEQUENCE时填写
}
 */
// _do('POST', '/projects/jk_test/topics/foo/shards/0', {
//     Action: "cursor",
//     Type: "LATEST"
// })

/**
 * 写入数据 - 不按shard写入
 * POST /projects/<ProjectName>/topics/<TopicName>/shards HTTP/1.1
 * {
        "Action": "pub",
        "Records": [
            {
                "ShardId": "0",
                "Attributes": {
                    "attr1": "value1",
                    "attr2": "value2"
                },
                "Data": ["A","B","3","4"] 
                //  "Data": "Base64String" // BLOB
            }
        ]
    }
 */
// _do('POST', '/projects/jk_test/topics/foo/shards', {
//     Action: "pub",
//     Records: [
//         {
//             ShardId: "0",
//             // Attributes: {
//             //     "attr1": "value1",
//             //     "attr2": "value2"
//             // },
//             Data: ["A","3"] 
//             //  "Data": "Base64String" // BLOB
//         }
//     ]
// })

/**
 * 读取数据
 * POST /projects/<ProjectName>/topics/<TopicName>/shards/<ShardId> HTTP/1.1
 * {
        "Action": "sub",
        "Cursor": "30005af19b3800000000000000000000",
        "Limit": 1
    }
    先查询 cursor
 */
// _do('POST', '/projects/jk_test/topics/foo/shards/0', {
//     Action: "sub",
//     Cursor: "30005f47ca6a00000000000000000000",
//     Limit: 10
// })

/**
 * 新增Field
 * POST /projects/<ProjectName>/topics/<TopicName> HTTP/1.1
 * {
        "Action": "appendfield",
        "FieldName": "field1",
        "FieldType": "BIGINT"
    }
 */
// _do('POST', '/projects/jk_test/topics/foo',{
//     Action: "appendfield",
//     FieldName: "field3",
//     FieldType: "STRING"
// })

// ============== Connector 数据同步 ===============
/**
 * 创建Connector
 * POST /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
 */


/**
 * 查询Connector
 * GET /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
 */

/**
 * 查询Connector列表
 * GET /projects/<ProjectName>/topics/<TopicName>/connectors HTTP/1.1
 */

/**
 * 删除Connector
 * DELETE /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
 */

/**
 * Reload Connector
 * POST /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
 * {
        "Action": "reload"
    }
 */

/**
 * 获取Connector Shard状态信息
 * POST /projects/<ProjectName>/topics/<TopicName>/connectors/<ConnectorType> HTTP/1.1
 * {
        "Action": "status",
        "ShardId": "0"
    }
 */

/**
 * Append Connector Field
 * POST /projects//topics//connectors/ HTTP/1.1
 */



// ============ subscription 数据订阅: 用于服务端存储指针 ==============
/**
 * 创建订阅
 * POST /projects/<ProjectName>/topics/<TopicName>/subscriptions HTTP/1.1
 * {
        "Action": "create",
        "Comment": "xxxx"
    }
 */
// _do('POST','/projects/jk_test/topics/foo/subscriptions',
// {
//     Action: "create",
//     Comment: 'create from api'
// })

/**
 * 查询订阅
 * GET /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubId> HTTP/1.1
 */
// _do('GET','/projects/jk_test/topics/foo/subscriptions/1598602034559OYGWN')

/**
 * 查询订阅列表
 * POST /projects/<ProjectName>/topics/<TopicName>/subscriptions HTTP/1.1
 * {
        "Action": "list",
        "PageIndex": 1,
        "PageSize": 10
    }
 */
// _do('POST','/projects/jk_test/topics/foo/subscriptions',
// {
//     "Action": "list",
//     "PageIndex": 1,
//     "PageSize": 10
// })

/**
 * 删除订阅
 * DELETE /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubId> HTTP/1.1
 */
// _do('DELETE', '/projects/jk_test/topics/foo/subscriptions/1598600157285CIFF5')


/**
 * 更新订阅状态
 * PUT /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubId> HTTP/1.1
 */
// _do('PUT','/projects/jk_test/topics/foo/subscriptions/1598602034559OYGWN',{
//     "State": 1
// })

/**
 * open点位session
 * POST /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubId>/offsets HTTP/1.1
 * {
        "Action": "open",
        "ShardIds": ["0"]
    }
 */
// _do('POST','/projects/jk_test/topics/foo/subscriptions/1598602034559OYGWN/offsets',{
//     "Action": "open",
//     "ShardIds": ["0"]
// })

/**
 * 查询点位
 * POST /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubId>/offsets HTTP/1.1
 * {
        "Action": "get",
        "ShardIds": ["0"]
    }
 */
// _do('POST','/projects/jk_test/topics/foo/subscriptions/1598602034559OYGWN/offsets',{
//     "Action": "get",
//     "ShardIds": ["0"]
// })

/**
 * 提交点位
 * PUT /projects/<ProjectName>/topics/<TopicName>/subscriptions/<SubId>/offsets HTTP/1.1
 * {
        "Action": "commit",
        "Offsets": {
            "0": {
                "Timestamp": 1000,
                "Sequence": 1,
                "Version": 1,
                "SessionId": 1
            }
        }
    }
 */
// _do('PUT','/projects/jk_test/topics/foo/subscriptions/1598602034559OYGWN/offsets',{
//     "Action": "commit",
//     "Offsets": {
//         "0": {
//             "Timestamp": 100000000000,
//             "Sequence": 2,
//             "Version": 0,
//             "SessionId": 1
//         }
//     }
// })