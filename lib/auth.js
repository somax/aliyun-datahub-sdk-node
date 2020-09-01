
/**
 * 认证与签名
 */
// 官方API参考文档 https://help.aliyun.com/document_detail/158856.html
const crypto = require('crypto');
/**
 * 构建规范头
 * @param {Object} headers 
 */
function _buildCanonicalizedDataHubHeaders(headers) {
    let _canonicalizedHeaders = '';
    let _headersToSign = {};
    for (const k in headers) {
        // 1. 将所有以 x-datahub- 为前缀的HTTP请求头的名字转换成小写 。如X-DATAHUB-Client-Version:1.1需要转换成x-datahub-client-version:1.1。
        let _k = k.toLowerCase();
        if (_k.indexOf('x-datahub-') === 0) {
            _headersToSign[_k] = headers[k];
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
 * @param {String} url 
 */
function _buildCanonicalizedResource(url) {
    // 1. 将CanonicalizedResource置成空字符串 “”；
    let _canonicalizedResource = '';

    // 2. 放入要访问的DataHub资源，如某个topic： /projects/test_project/topics/foo
    _canonicalizedResource += `${url}`;

    // 3. 如果请求的资源包含额外的url参数，按照字典序，从小到大排列并以 & 为分隔符生成参数字符串。
    //    在CanonicalizedResource字符串尾添加 ？和参数字符串。
    //    此时的CanonicalizedResource如：/projects/test_project/topics/foo/connectors/sink_odps?donetime
    // TODO 暂时还未涉及到有参数的 API, 后续添加
    // let hasQuery = false; 
    // if (hasQuery) {
    //     let _orderedParams = '';
    //     _canonicalizedResource += `?${_orderedParams}`;
    // }

    return _canonicalizedResource;
}

/**
 * 构建规范字符串
 * @param {String} method 
 * @param {String} url 
 * @param {Object} headers 
 */
function _buildCanonicalStr(method, url, headers) {
    let contentType = headers['Content-Type'];
    let date = headers.Date;
    let canonicalizedDataHubHeaders = _buildCanonicalizedDataHubHeaders(headers);
    let canonicalizedResource = _buildCanonicalizedResource(url);
    let _canonicalStr = `${method}\n${contentType}\n${date}\n${canonicalizedDataHubHeaders}\n${canonicalizedResource}`;
    return _canonicalStr;
}

/**
 * 生成签名
 * @param {String} method 
 * @param {String} url 
 * @param {Object} headers 
 * @param {String} accessKeySecret 
 */
function _buildSignature(method, url, headers, accessKeySecret) {
    // let canonicalStr = _buildCanonicalStr(reqOption);
    // let {method, url, headers} = reqOption
    let canonicalStr = _buildCanonicalStr(method, url, headers);
    let _sign = crypto
        .createHmac('sha1', accessKeySecret)
        .update(canonicalStr)
        .digest()
        .toString('base64');
    return _sign;
}

/**
 * 添加 Auth 头
 * @param {String} method 
 * @param {String} url 
 * @param {Object} headers 
 * @param {String} accessKeyId 
 * @param {String} accessKeySecret 
 */
function _buildAuthorization(method, url, headers, accessKeyId, accessKeySecret) {
    if (!accessKeyId || !accessKeySecret) {
        throw new Error('Missing ACCESS_KEY_ID / ACCESS_KEY_SECRET');
    }
    let _sign = _buildSignature(method, url, headers, accessKeySecret);
    return `DATAHUB ${accessKeyId}:${_sign}`;
}

class Auth{
    /**
     * 认证签名
     * @param {String} accessKeyId 
     * @param {String} accessKeySecret 
     */
    constructor(accessKeyId, accessKeySecret){
        this.accessKeyId = accessKeyId
        this.accessKeySecret = accessKeySecret
    }
    addAuthHeader(method, url, headers){
        headers.Authorization = _buildAuthorization(method, url, headers, this.accessKeyId, this.accessKeySecret)
        return headers
    }
}

module.exports = Auth