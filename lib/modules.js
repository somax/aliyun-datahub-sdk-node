const { FieldType } = require('./statics');
class RequestOptions {
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
    constructor(endpoint, accessKeyId, accessKeySecret) {
        this.ENDPOINT = endpoint
        this.ACCESS_KEY_ID = accessKeyId
        this.ACCESS_KEY_SECRET = accessKeySecret
    }
}

class Field{
    constructor(name, type, notnull = false){
        this.name = name
        this.type = type
        this.notnull = notnull
    }
}

module.exports = {
    RequestOptions,
    DatahubOptions,
    Field
}