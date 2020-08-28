const axios = require("axios");
const crypto = require("crypto");
const { RecordType } = require('./record');

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
    console.debug('[DEBUG]', { _headersToSign });

    // 3. 将上一步得到的所有HTTP请求头按照名字的字典序进行升序排列。
    let _orderedKeys = Object.keys(_headersToSign).sort();
    console.debug('[DEBUG]', { _orderedKeys });

    if (_orderedKeys.length > 0) {
        // 4. 删除请求头和内容之间分隔符两端出现的任何空格。如x-datahub-client-versionn : 1.1转换成：x-datahub-client-version:1.1。
        let _orderedHeadersToSign = _orderedKeys.map(
            (_item) => `${_item}:${_headersToSign[_item]}`
        );
        console.debug('[DEBUG]', {
            _headersToSign_ordered: _orderedHeadersToSign,
        });

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
    // TODO 暂时还未涉及到有参数的 API, 后续添加
    // let hasQuery = false; 
    // if (hasQuery) {
    //     let _orderedParams = "";
    //     _canonicalizedResource += `?${_orderedParams}`;
    // }

    console.debug('[DEBUG]', { _canonicalizedResource });
    return _canonicalizedResource;
}

function buildCanonicalStr(reqOption) {
    let HTTPMethod = reqOption.method;
    let ContentType = reqOption.headers["Content-Type"];
    let date = reqOption.headers.Date;
    let canonicalizedDataHubHeaders = buildCanonicalizedDataHubHeaders(
        reqOption
    );
    let canonicalizedResource = buildCanonicalizedResource(reqOption);
    let _canonicalStr = `${HTTPMethod}\n${ContentType}\n${date}\n${canonicalizedDataHubHeaders}\n${canonicalizedResource}`;
    console.debug('[DEBUG]', { _canonicalStr });
    return _canonicalStr;
}

/**
 * 生成签名
 */
function buildSignature(reqOption, accessKeySecret) {
    let canonicalStr = buildCanonicalStr(reqOption);
    let _sign = crypto
        .createHmac("sha1", accessKeySecret)
        .update(canonicalStr)
        .digest()
        .toString("base64");
    return _sign;
}


function buildAuthorization(reqOption, accessKeyId, accessKeySecret) {
    if (!accessKeyId || !accessKeySecret) {
        throw new Error("Missing ACCESS_KEY_ID / ACCESS_KEY_SECRET");
    }
    let _sign = buildSignature(reqOption, accessKeySecret);
    console.debug('[DEBUG]', { sign: _sign });
    reqOption.headers.Authorization = `DATAHUB ${accessKeyId}:${_sign}`;
    return reqOption;
}

async function _do(restClient, method, url, data) {
    let _option = {
        method,
        url,
        headers: {
            "Content-Type": "application/json;",
            Date: new Date().toGMTString(),
            "x-datahub-client-version": "1.1",
        },
        data,
    };

    let _reqOption = buildAuthorization(_option, restClient.ACCESS_KEY_ID, restClient.ACCESS_KEY_SECRET);

    console.debug('[DEBUG]', _reqOption);

    try {
        const response = await restClient(_reqOption);
        console.log('[INFO]', response.data);
    } catch (error) {
        if (error.response) {
            console.error(
                '[ERROR]',
                error.response.status,
                error.response.statusText,
                error.response.data
            );
        } else {
            console.error('[ERROR]', error);
        }
    }
}

class Datahub {
    constructor(option) {
        this.restClient = axios.create({
            baseURL: option.ENDPOINT,
            timeout: 1000,
        });

        this.restClient.ACCESS_KEY_ID = option.ACCESS_KEY_ID;
        this.restClient.ACCESS_KEY_SECRET = option.ACCESS_KEY_SECRET;

        // this.RecordType = RecordType

    }


    createProject(projectName, comment = '') {
        _do(this.restClient, 'POST', `/projects/${projectName}`, { Comment: comment });
    }

    getProject(projectName) {
        _do(this.restClient, 'GET', `/projects/${projectName}`);
    }

    updateProject(projectName, comment) {
        _do(this.restClient, 'PUT', `/projects/${projectName}`, { Comment: comment });
    }

    deleteProject(projectName) {
        _do(this.restClient, 'DELETE', `/projects/${projectName}`);
    }

    listProjects() {
        _do(this.restClient, 'GET', `/projects`);
    }


    createTopic(projectName, topicName, recordSchema, comment = '', recordType = RecordType.TUPLE, shardCount = 1, lifeCycle = 1) {
        // {
        //     "Action": "create",
        //     "ShardCount": 1,
        //     "Lifecycle": 1,
        //     "RecordType": "TUPLE",
        //     "RecordSchema": "{\"fields\":[{\"name\":\"field1\",\"type\":\"STRING\"},{\"name\":\"field2\",\"type\":\"BIGINT\"}]}",
        //     "Comment": "create topic"
        // }
        _do(this.restClient, 'POST', `/projects/${projectName}/topics/${topicName}`, {
            Action: 'create',
            ShardCount: shardCount,
            Lifecycle: lifeCycle,
            RecordType: recordType,
            RecordSchema: JSON.stringify(recordSchema),
            Comment: comment
        })
    }

    getTopic(projectName, topicName) {
        _do(this.restClient, 'GET', `/projects/${projectName}/topics/${topicName}`)
    }

    updateTopic(projectName, topicName) {

    }

    deleteTopic() {

    }

    ListTopic() {

    }

}
Datahub.RecordType = RecordType
    

module.exports = Datahub;
