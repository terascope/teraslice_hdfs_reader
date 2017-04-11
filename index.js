'use strict';

var Promise = require('bluebird');
var path = require('path');
var Queue = require('./lib/queue');
var fs = require('fs')


function getClient(context, config, type) {
    var clientConfig = {};
    clientConfig.type = type;

    if (config && config.hasOwnProperty('connection')) {
        clientConfig.endpoint = config.connection ? config.connection : 'default';
        clientConfig.cached = config.connection_cache !== undefined ? config.connection_cache : true;

    }
    else {
        clientConfig.endpoint = 'default';
        clientConfig.cached = true;
    }

    return context.foundation.getConnection(clientConfig);
}


var parallelSlicers = false;

function newSlicer(context, job, retryData, slicerAnalytics, logger) {
    let opConfig = getOpConfig(job.jobConfig, 'teraslice_hdfs_reader');
    let clientService = getClient(context, opConfig, 'hdfs_ha');
    let client = clientService.client
    let queue = new Queue();

    function processFile(file, path) {
        console.log('what file', file);
        let totalLength = file.length;
        let fileSize = file.length

        if (fileSize <= opConfig.size) {
            queue.enqueue({path: `${path}/${file.pathSuffix}`, fullChunk: true})
        }
        else {
            let offset = 0;
            while (fileSize > 0) {
                let length = fileSize > opConfig.size ? opConfig.size : fileSize;
                queue.enqueue({
                    path: `${path}/${file.pathSuffix}`,
                    offset: offset,
                    length: length,
                    total: totalLength
                });
                fileSize -= opConfig.size;
                offset += length;
            }
        }
    }

    function getFilePaths(path) {
        return client.listStatusAsync(path)
            .then(function(results) {
                return Promise.map(results, function(metadata) {
                    if (metadata.type === "FILE") {
                        return processFile(metadata, path)
                    }

                    if (metadata.type === "DIRECTORY") {
                        return getFilePaths(`${path}/${metadata.pathSuffix}`)
                    }

                    return true
                })
            })
            .then(function() {
                return [()=> queue.dequeue()]
            })
            .catch(function(err) {
                if (err.exception === "StandbyException") {
                    return Promise.reject({initialize: true})
                }
                else {
                    var errMsg = err.stack;
                    return Promise.reject(`Error while attempting to read from hdfs path: ${path}, error: ${errMsg}`);
                }

            })
    }

    return getFilePaths(opConfig.path)
        .catch(function(err) {
            if (err.initialize) {
                logger.warn(`hdfs namenode has changed, reinitializing client`);
                var newClient = clientService.changeNameNode().client;
                client = newClient;
                return getFilePaths(opConfig.path)
            }
            else {

                var errMsg = err.stack ? err.stack : err
                logger.error(`Error while reading from hdfs, error: ${errMsg}`)
                return Promise.reject(err)
            }
        })
}


function newReader(context, opConfig, jobConfig) {
    let clientService = getClient(context, opConfig, 'hdfs_ha');
    let client = clientService.client

    return function readChunk(msg, logger) {
        return determineChunk(client, msg, logger)
            .catch(function(err) {
                if (err.initialize) {
                    logger.warn(`hdfs namenode has changed, reinitializing client`);
                    client = clientService.changeNameNode().client;
                    return readChunk(msg, logger)
                }
                else {
                    var errMsg = parseError(err)
                    logger.error(errMsg)
                    return Promise.reject(err)
                }
            })
    };
}

function parseError(err) {
    console.log('what error do we have here', err.message, err.exception)
    if (err.message && err.exception) {
        return `Error while reading from hdfs, error: ${err.exception}, ${err.message}`
    }
}

function getChunk(client, msg, options) {
    if (msg.total) {
        if (msg.total <= options.offset) {
            //the last slice will try to over shoot, just return an empty string
            return Promise.resolve('')
        }
    }

    return client.openAsync(msg.path, options, {json: true})
        .then(function(results) {
            if (results === '{"RemoteException":{"exception":"StandbyException","javaClassName":"org.apache.hadoop.ipc.StandbyException","message":"Operation category READ is not supported in state standby. Visit https://s.apache.org/sbnn-error"}}') {
                return Promise.reject()
            }
            else {
                return results
            }
        })
}

function determineChunk(client, msg, logger) {
    let options = {};

    if (msg.length) {
        options.offset = msg.offset;
        options.length = msg.length;
    }

    return getChunk(client, msg, options)
        .then(function(str) {
            let allRecordsIntact = str[str.length - 1] === '\n' ? true : false;
            let dataList = str.split("\n");
            if (msg.fullChunk) {
                return dataList.map(chunk => JSON.parse(chunk))
            }
            else {
                //TODO hard bound length to 500, improve this
                let nextChunk = {offset: msg.offset + msg.length, length: 500}

                return getChunk(client, msg, nextChunk)
                    .then(function(nextStr) {
                        // logger.error('next str', nextStr)
                        let nextNewLine = nextStr.search(/\n/);
                        let nextDoc = nextStr.slice(0, nextNewLine);

                        //artifact of splitting, may have an empty string at the end of the array
                        if (dataList[dataList.length - 1].length === 0) {
                            dataList.pop()
                        }

                        if (nextDoc.length > 0) {
                            if (allRecordsIntact) {
                                dataList.push(nextDoc)
                            }
                            else {
                                //concat the last doc of the array together to complete the record
                                dataList[dataList.length - 1] = dataList[dataList.length - 1] + nextDoc
                            }
                        }

                        if (msg.offset !== 0) {
                            //TODO review this as it can be really expensive
                            dataList.shift()
                        }
                        console.log(dataList)
                        return dataList.map(chunk => chunk)
                    })
                    .catch(function(err) {
                        if (err.exception === "StandbyException") {
                            return Promise.reject({initialize: true})
                        }
                        else {
                            var errMsg = err.stack;
                            return Promise.reject(`Error while attempting process hdfs slice: ${JSON.stringify(msg)} on hdfs, error: ${errMsg}`);
                        }
                    })
            }
        })
        .catch(function(err) {
            if (err.exception === "StandbyException" || err.initialize) {
                return Promise.reject({initialize: true})
            }
            else {
                var errMsg = err.stack;
                return Promise.reject(`Error while attempting process hdfs slice: ${JSON.stringify(msg)} on hdfs, error: ${errMsg}`);
            }
        })
}

function json_lines(str) {

}

function chunkType(opConfig) {
    if (opConfig.format === 'json_lines') {
        return json_lines
    }
}

function chunker(opConfig) {
    var chunkFormater = chunkType(opConfig)
    return function(msg, str) {
        //is a complete file
        if (msg.fullChunk) {
            return chunkFormater(str)
        }
        else {
            //if offset is zero, its the start of the slice
            //if(msg.offset === 0){
            return chunkFormater(str)
            //}

        }
    }
}


function getOpConfig(job, name) {
    return job.operations.find(function(op) {
        return op._op === name;
    })
}


function schema() {
    return {
        user: {
            doc: 'User to use when writing the files. Default: "hdfs"',
            default: 'hdfs',
            format: 'optional_String'
        },
        namenode_list: {
            doc: 'A list containing all namenode_hosts, this option is needed for high availability',
            default: []
        },
        path: {
            doc: "HDFS location to process. Most of the time this will be a directory that contains multiple files",
            default: '',
            format: function(val) {
                if (typeof val !== 'string') {
                    throw new Error('path in teraslice_hdfs_reader must be a string')
                }

                if (val.length === 0) {
                    throw new Error('path in teraslice_hdfs_reader must specify a valid path in hdfs')
                }
            }
        },
        size: {
            doc: "How big of a slice to take out of each file",
            default: 2524,
            format: Number
        },
        format: {
            doc: "For now just supporting json_lines but other formats may make sense later.",
            default: "json_lines",
            format: ["json_lines"]
        }
    };
}


module.exports = {
    newReader: newReader,
    newSlicer: newSlicer,
    schema: schema,
    parallelSlicers: parallelSlicers
};