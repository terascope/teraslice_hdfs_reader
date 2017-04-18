'use strict';

var Promise = require('bluebird');
var path = require('path');
var Queue = require('queue');
var fs = require('fs');


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
    let client = clientService.client;
    let queue = new Queue();

    function processFile(file, path) {
        let totalLength = file.length;
        let fileSize = file.length;

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
                var errMsg = parseError(err);
                return Promise.reject(errMsg);
            })
    }

    return getFilePaths(opConfig.path)
        .catch(function(err) {
            var errMsg = parseError(err);
            logger.error(`Error while reading from hdfs, error: ${errMsg}`);
            return Promise.reject(errMsg)
        })
}


function newReader(context, opConfig, jobConfig) {
    let clientService = getClient(context, opConfig, 'hdfs_ha');
    let client = clientService.client;
    let chunkFormater = chunkType(opConfig);

    return function readChunk(msg, logger) {
        return determineChunk(client, msg, logger)
            .then(function(results) {
                return chunkFormater(results)
            })
            .catch(function(err) {
                var errMsg = parseError(err);
                logger.error(errMsg);
                return Promise.reject(err);
            })
    };
}

function parseError(err) {
    if (err.message && err.exception) {
        return `Error while reading from hdfs, error: ${err.exception}, ${err.message}`
    }
    return `Error while reading from hdfs, error: ${err}`
}

function getChunk(client, msg, options) {
    if (msg.total) {
        if (msg.total <= options.offset) {
            //the last slice will try to over shoot, just return an empty string
            return Promise.resolve('')
        }
    }

    return client.openAsync(msg.path, options)
}

function averageDocSize(array) {
    return Math.floor(array.reduce((accum, str)=> {
            return accum + str.length;
        }, 0) / array.length)
}

function nextChunk(slice, avgLength) {
    let newStart = slice.offset + slice.length;
    let newLength = (newStart + avgLength) < slice.total ? avgLength : slice.total - newStart;
    return {offset: newStart, length: newLength, total: slice.total};
}

function getNextDoc(client, msg, avgLength) {
    let nextDocOptions = nextChunk(msg, avgLength);

    function getDoc(client, msg, options) {
        return getChunk(client, msg, options)
            .then(function(chunk) {
                if (chunk.search(/\n/) !== -1 || chunk.length === 0) {
                    return chunk
                }
                else {
                    let nextChunkOptions = nextChunk(options, avgLength);
                    return getDoc(client, msg, nextChunkOptions)
                }
            })
    }

    return getDoc(client, msg, nextDocOptions);
}

function determineChunk(client, msg, logger) {
    let options = {};

    if (msg.length) {
        options.offset = msg.offset;
        options.length = msg.length;
    }

    return getChunk(client, msg, options)
        .then(function(str) {
            let allRecordsIntact = str[str.length - 1] === '\n';
            let dataList = str.split("\n");
            if (msg.fullChunk) {
                return dataList;
            }
            else {
                let newLength = averageDocSize(dataList) * 2;

                return getNextDoc(client, msg, newLength)
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
                                dataList[dataList.length - 1] = dataList[dataList.length - 1] + nextDoc;
                            }
                        }

                        if (msg.offset !== 0) {
                            dataList.shift()
                        }

                        return dataList;
                    })
                    .catch(function(err) {
                        var errMsg = err.stack;
                        return Promise.reject(`Error while attempting process hdfs slice: ${JSON.stringify(msg)} on hdfs, error: ${errMsg}`);
                    })
            }
        })
        .catch(function(err) {
            var errMsg = err.stack;
            return Promise.reject(`Error while attempting process hdfs slice: ${JSON.stringify(msg)} on hdfs, error: ${errMsg}`);
        })
}

function json_lines(data) {
    return data.map(record => JSON.parse(record))
}

function defaultLines(data) {
    return data
}

function chunkType(opConfig) {
    if (opConfig.format === 'json_lines') {
        return json_lines
    }
    return defaultLines
}


function getOpConfig(job, name) {
    return job.operations.find(function(op) {
        return op._op === name;
    })
}


function schema() {
    return {
        user: {
            doc: 'User to use when reading the files. Default: "hdfs"',
            default: 'hdfs',
            format: 'optional_String'
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
            doc: "How big of a slice in bytes to take out of each file.",
            default: 100000,
            format: Number
        },
        format: {
            doc: "For now just supporting json_lines but other formats may make sense later.",
            default: "json_lines",
            format: ["json_lines"]
        },
        connection: {
            doc: 'Name of the HDFS connection to use.',
            default: 'default',
            format: 'optional_String'
        }
    };
}


module.exports = {
    newReader: newReader,
    newSlicer: newSlicer,
    schema: schema,
    parallelSlicers: parallelSlicers
};

