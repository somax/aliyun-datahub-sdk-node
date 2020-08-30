module.exports = {
    RecordType: {
        TUPLE: 'TUPLE',
        BOLB: 'BOLB'
    },
    FieldType: {
        STRING: 'string',
        BOOLEAN:'boolean',
        TINYINT:'tinyint',
        SMALLINT:'smallint',
        INTEGER:'integer',
        BIGINT: 'bigint',
        FLOAT:'float',
        DOUBLE:'double',
        TIMESTAMP:'timestamp',
        DECIMAL:'decimal',
        convertToJavaScriptType(value, type) {
            switch (type.toLowerCase()) {
                case this.BOOLEAN:
                    let _v = value.toLowerCase()
                    if(_v === 'true'){
                        return true
                    }else{
                        if(_v === 'false'){
                            return false
                        }else{
                            throw Error('InvalidBooleanValue')
                        }
                    }
                case this.TINYINT:
                case this.SMALLINT:
                case this.INTEGER:
                case this.BIGINT:
                case this.DECIMAL:
                case this.DOUBLE:
                    return Number(value)
                case this.TIMESTAMP:
                    return new Date(Number(value))
                default:
                    return String(value)
            }
        },
        convertToDatahubType(value, type){
            switch (type.toLowerCase()) {
                case this.TIMESTAMP:
                    return String(new Date(value).getTime())
                default:
                    return String(value)
            }
        }
    }
}