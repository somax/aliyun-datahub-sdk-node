/**
 * Rest 客户端
 */
const axios = require('axios');
const Auth = require('./auth');
const {RequestOptions} =require('./modules');


class RestClient {
    /**
     * Http 客户端
     * @param {String} baseURL 
     * @param {String} accessKeyId 
     * @param {String} accessKeySecret 
     * @function request 
     */
    constructor(baseURL, accessKeyId, accessKeySecret) {
        this.client = axios.create({
            baseURL,
            timeout: 1000,
        });
        this.auth =  new Auth(accessKeyId, accessKeySecret)
    }
    /**
     * 
     * @param {String} method 
     * @param {String} url 
     * @param {Object>} data 
     */
    async request(method, url, data) {
        let _reqOption = new RequestOptions(method, url, data)
        _reqOption.headers = this.auth.addAuthHeader(method, url, _reqOption.headers)

        try {
            const response = await this.client(_reqOption);
            let { status, statusText, data } = response
            return { status, statusText, data }
        } catch (error) {
            if (error.response) {
                let { status, statusText, data } = error.response
                let { ErrorCode, ErrorMessage } = data
                throw new Error(`[${status} ${statusText}] ${ErrorCode}: ${ErrorMessage}`)
            } else {
                throw error
            }
        }
    }
}



module.exports = RestClient