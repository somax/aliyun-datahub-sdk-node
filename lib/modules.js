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
RecordType.BOLB = 'BOLB'

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
            return new Date(Number(value))
        default:
            return String(value)
    }
}
FieldType.convertToDatahubType = (value, type) => {
    switch (type.toLowerCase()) {
        case FieldType.TIMESTAMP:
            return String(new Date(value).getTime())
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


/**
 * 
 * @param {Object} data 
 * @param {RecordSchema} schema 
 */
function _data2Record(data, schema) {
    let record = []
    schema.fields.forEach((item, index) => {
        record[index] = FieldType.convertToDatahubType(data[item.name], item.type)
    });
    return record
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
        this.Data = _data2Record(data, schema)
    }

}


module.exports = {
    RequestOptions,
    DatahubOptions,
    Field,
    RecordSchema,
    RecordType,
    FieldType,
    TupleRecord
}