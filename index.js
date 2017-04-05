'use strict';


//var _ = require('lodash');
var Promise = require('bluebird');
var path = require('path');
var Queue = require('./lib/queue');
var readline = require('readline');
var fs = require('fs')
/*
 Configuration
 // These should probably be terafoundation level configs
 namenode host
 namenode port
 user
 Overall this should be a plugin of some sort
 */


function newProcessor(context, opConfig, jobConfig) {
    var logger = context.logger;
    var canMakeNewClient = opConfig.namenode_list.length >= 2;
    var endpoint = opConfig.connection ? opConfig.connection : 'default';
    var nodeNameHost = context.sysconfig.terafoundation.connectors.hdfs[endpoint].namenode_host;

    //client connection cannot be cached, an endpoint needs to be re-instantiated for a different namenode_host
    opConfig.connection_cache = false;

    var client = getClient(context, opConfig, 'hdfs');
    var hdfs = Promise.promisifyAll(client);

    function prepare_file(hdfs, filename, chunks, logger) {
        // We need to make sure the file exists before we try to append to it.
        return hdfs.getFileStatusAsync(filename)
            .catch(function(err) {
                // We'll get an error if the file doesn't exist so create it.
                return hdfs.mkdirsAsync(path.dirname(filename))
                    .then(function(status) {
                        logger.warn("I should be creating a file")
                        return hdfs.createAsync(filename, '');
                    })
                    .catch(function(err) {
                        if (canMakeNewClient && err.exception === "StandbyException") {
                            return Promise.reject({initialize: true})
                        }
                        else {
                            var errMsg = err.stack;
                            return Promise.reject(`Error while attempting to create the file: ${filename} on hdfs, error: ${errMsg}`);
                        }
                    })
            })
            .return(chunks)
            // We need to serialize the storage of chunks so we run with concurrency 1
            .map(function(chunk) {
                if (chunk.length > 0) {
                    return hdfs.appendAsync(filename, chunk)
                }
            }, {concurrency: 1})
            .catch(function(err) {
                //for now we will throw if there is an async error
                if (err.initialize) {
                    return Promise.reject(err)
                }
                var errMsg = err.stack ? err.stack : err
                return Promise.reject(`Error sending data to file: ${filename}, error: ${errMsg}, data: ${JSON.stringify(chunks)}`)
            })
    }

    return function(data, logger) {
        var map = {};
        data.forEach(function(record) {
            if (!map.hasOwnProperty(record.filename)) map[record.filename] = [];

            map[record.filename].push(record.data)
        });

        function sendFiles() {
            var stores = [];
            _.forOwn(map, function(chunks, key) {
                stores.push(prepare_file(hdfs, key, chunks, logger));
            });

            // We can process all individual files in parallel.
            return Promise.all(stores)
                .catch(function(err) {
                    if (err.initialize) {
                        logger.warn(`hdfs namenode has changed, reinitializing client`);
                        var newClient = makeNewClient(context, opConfig, nodeNameHost, endpoint);
                        hdfs = newClient.hdfs;
                        nodeNameHost = newClient.nodeNameHost;
                        return sendFiles()
                    }

                    var errMsg = err.stack ? err.stack : err
                    logger.error(`Error while sending to hdfs, error: ${errMsg}`)
                    return Promise.reject(err)
                });
        }

        return sendFiles();
    }
}

function makeNewClient(context, opConfig, conn, endpoint) {
    var list = opConfig.namenode_list;
    //we want the next spot
    var index = list.indexOf(conn) + 1;
    //if empty start from the beginning of the
    var nodeNameHost = list[index] ? list[index] : list[0];

    //TODO need to review this, altering config so getClient will start with new namenode_host
    context.sysconfig.terafoundation.connectors.hdfs[endpoint].namenode_host = nodeNameHost
    return {
        nodeNameHost: nodeNameHost,
        hdfs: Promise.promisifyAll(getClient(context, opConfig, 'hdfs'))
    }
}

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

    return context.foundation.getConnection(clientConfig).client;
}


var parallelSlicers = false;

function newSlicer(context, job, retryData, slicerAnalytics, logger) {
    let opConfig = getOpConfig(job.jobConfig, 'teraslice_hdfs_reader');
    let client = Promise.promisifyAll(getClient(context, opConfig, 'hdfs'));
    let hdfsPath = opConfig.path

    let queue = new Queue();

    return client.listStatusAsync(opConfig.path)
        .then(function(results) {
            //TODO deal recursively with directories
            let fileList = results.filter(function(obj) {
                return obj.type === "FILE"
            })

            fileList.forEach(function(file) {
                let totalLength = file.length;
                let fileSize = file.length

                if (fileSize <= opConfig.size) {
                    queue.enqueue({path: `${hdfsPath}/${file.pathSuffix}`, fullChunk: true})
                }
                else {
                    let offset = 0;
                    while (fileSize > 0) {
                        let length = fileSize > opConfig.size ? opConfig.size : fileSize;
                        queue.enqueue({
                            path: `${hdfsPath}/${file.pathSuffix}`,
                            offset: offset,
                            length: length,
                            total: totalLength
                        });
                        fileSize -= opConfig.size;
                        offset += length;
                    }
                }
            })

            return [()=> queue.dequeue()]
        })
        .catch(function(err) {
            console.log('the error', err)
        })

}


function newReader(context, opConfig, jobConfig) {
    let client = Promise.promisifyAll(getClient(context, opConfig, 'hdfs'));
    let processChunk = chunker(opConfig)
    return function(msg, logger) {
        //if length is given, fill should not be read in one go
        //logger.error('the orig message', msg)

        return determineChunk(client, msg, logger)
    };
}

function getChunk(client, msg, options) {
    if (msg.total) {
        if (msg.total <= options.offset) {
            return Promise.resolve('')
        }
    }

    return client.openAsync(msg.path, options)
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

                //TODO need to deal with the condition of last chunk, dont need to query again
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
                        
                        return dataList.map(chunk => JSON.parse(chunk))
                    })
                    .catch(function(err) {
                        logger.error('what error is this', err)
                    })
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