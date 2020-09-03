/**
 * Datahub 相关模块
 */
class RequestOptions {
    /**
     * Rest Client Request Options
     * @param {String} method 
     * @param {String} url 
     * @param {Object} data 
     */
    constructor(method, url, data) {
        this.method = method || 'GET'
        this.url = url || '/'
        this.headers = {
            'Content-Type': 'application/json;',
            Date: new Date().toGMTString(),
            'x-datahub-client-version': '1.1',
        }
        this.data = data || {}
    }
}
class DatahubOptions {
    /**
     * Datahub Options
     * @param {String} endpoint 
     * @param {String} accessKeyId 
     * @param {String} accessKeySecret 
     */
    constructor(endpoint, accessKeyId, accessKeySecret) {
        this.ENDPOINT = endpoint
        this.ACCESS_KEY_ID = accessKeyId
        this.ACCESS_KEY_SECRET = accessKeySecret
    }
}

class RecordType {
}
RecordType.TUPLE = 'TUPLE'
RecordType.BLOB = 'BLOB'

class FieldType {

}
FieldType.STRING = 'string'
FieldType.BOOLEAN = 'boolean'
FieldType.TINYINT = 'tinyint'
FieldType.SMALLINT = 'smallint'
FieldType.INTEGER = 'integer'
FieldType.BIGINT = 'bigint'
FieldType.FLOAT = 'float'
FieldType.DOUBLE = 'double'
FieldType.TIMESTAMP = 'timestamp'
FieldType.DECIMAL = 'decimal'
FieldType.convertToJavaScriptType = (value, type) => {
    switch (type.toLowerCase()) {
        case FieldType.BOOLEAN:
            let _v = value.toLowerCase()
            if (_v === 'true') {
                return true
            } else {
                if (_v === 'false') {
                    return false
                } else {
                    throw Error('InvalidBooleanValue')
                }
            }
        case FieldType.TINYINT:
        case FieldType.SMALLINT:
        case FieldType.INTEGER:
        case FieldType.BIGINT:
        case FieldType.DECIMAL:
        case FieldType.DOUBLE:
            return Number(value)
        case FieldType.TIMESTAMP:
            // 先降级到毫秒
            return new Date(Number(value.substr(0,13)))
        default:
            return String(value)
    }
}
FieldType.convertToDatahubType = (value, type) => {
    switch (type.toLowerCase()) {
        case FieldType.TIMESTAMP:
            // 转换为微秒
            return String(new Date(value).getTime() * 1000) 
        default:
            return String(value)
    }
}

class Field {
    /**
     * 构建 Field
     * @param {String} name 
     * @param {RecordType} type 
     * @param {Boolean} notnull 
     */
    constructor(name, type, notnull = false) {
        // TODO check type if not support
        this.name = name
        this.type = type
        this.notnull = notnull
    }
}

class RecordSchema {
    /**
     * 构建 Record Schema
     * @example
     * new RecordSchema([ [ 'name', RecordType.STRING, true] ])
     * @param {Array<Field>} fields 
     */
    constructor(fields) {
        this.fields = fields.map(item => new Field(item[0], item[1], item[2]))
    }
}

class TupleRecord {
    /**
     * 
     * @param {RecordSchema} schema 
     * @param {Array<String>} data 
     * @param {Object} attributes 
     * @param {String} shardId 
     */
    constructor(data, schema, attributes = null, shardId = '0') {
        this.ShardId = shardId
        this.Attributes = attributes
        this.Data = TupleRecord.data2Record(data, schema)
    }
}
TupleRecord.parse = function (record, schema) {
    let data = {}
    schema.fields.forEach((item, index) => {
        data[item.name] = FieldType.convertToJavaScriptType(record[index], item.type)
    });
    return data
}
TupleRecord.data2Record = function (data, schema) {
    let record = []
    schema.fields.forEach((item, index) => {
        record[index] = FieldType.convertToDatahubType(data[item.name], item.type)
    });
    return record
}

class BlobRecord {
    constructor(data, attributes = null, shardId = '0') {
        this.ShardId = shardId
        this.Attributes = attributes
        this.Data = BlobRecord.base64Encode(data)
    }
}
BlobRecord.base64Encode = function (data) {
    return Buffer.from(data).toString('base64');
}
BlobRecord.base64Decode = function (data) {
    return Buffer.from(data, 'base64');
}

class CursorType { }
CursorType.OLDEST = 'OLDEST'
CursorType.LATEST = 'LATEST'
CursorType.SYSTEM_TIME = 'SYSTEM_TIME'
CursorType.SEQUENCE = 'SEQUENCE'


module.exports = {
    RequestOptions,
    DatahubOptions,
    Field,
    RecordSchema,
    RecordType,
    FieldType,
    TupleRecord,
    BlobRecord,
    CursorType
}