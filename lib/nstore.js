// Copyright 2010 Tim Caswell <tim@sencha.com>


var sys = require('sys'),
    fs = require('fs'),
    Buffer = require('buffer').Buffer;

// This size only affects performance, it's not a constraint on data sizes
var CHUNK_SIZE = 1024;
// This is the max size of a single serialized document
var MAX_SIZE = 1024 * 1024;


function nStore(filename) {
    var writer, reader; // FDs for reading and writing to the file
    var index = {};
    var writeQueue = [];
    var maxKey = 0;
    var dbLength = 0;

    // Open a single writer and reader
    writer = fs.openSync(filename, "a");
    reader = fs.openSync(filename, "r");

    // Loads the database from disk using blocking I/O
    function loadDatabase() {

        // Create a buffer for reading chunks from the disk
        var chunk = new Buffer(CHUNK_SIZE);

        // Create an empty stream buffer
        var input = new Buffer(MAX_SIZE);
        input.length = 0;

        // These are positions in the database file
        var offset = 0;
        var base = 0;

        // This is a position within the input stream
        var pos = 0;
        var mid = 0;

        // Read a chunk from the file into `chunk`
        while ((chunk.length = fs.readSync(reader, chunk, 0, CHUNK_SIZE, offset)) > 0) {

            // Move the offset so the outer loop stays in sync
            offset += chunk.length;

            // Copy the chunk onto the input stream
            chunk.copy(input, input.length, 0, chunk.length);
            input.length += chunk.length;

            // See if there is input to consume
            for (var i = pos, l = input.length; i < l; i++) {
                if (input[i] === 9) {
                    mid = i + 1;
                }
                if (mid && input[i] === 10) {
                    var meta = JSON.parse(input.slice(pos, mid - 1).toString());
                    var doc = input.slice(mid, i).toString();
                    meta.position = base + mid;
                    meta.length = i - mid;
                    index[meta.key] = meta;
                    mid = 0;
                    pos = i + 1;
                    // TODO: use
                }
            }

            // Shift the input back down
            if (pos > 0) {
                input.copy(input, 0, pos, input.length);
                input.length -= pos;
                base += pos;
                pos = 0;
            }
        }

        dbLength = offset;

        // Preset maxKey
        Object.keys(index).forEach(function (key) {
            if (/^[1-9][0-9]*$/.test(key)) {
                var val = parseInt(key, 10);
                if (val > maxKey) {
                    maxKey = val;
                }
            }
        });
    }

    function getByKey(key, callback) {
        try {
            var meta = index[key];
            if (meta.doc) {
                process.nextTick(function () {
                    meta.doc.key = key;
                    callback(null, meta.doc);
                });
                return;
            }
            var buffer = new Buffer(meta.length);

            var offset = 0;
            function getNext() {
                fs.read(reader, buffer, offset, meta.length - offset, meta.position + offset, function (err, bytes) {
                    if (err) {
                        callback(err);
                        return;
                    }
                    try {
                        if (bytes < meta.length - offset) {
                            offset += bytes;
                            getNext();
                            return;
                        }
                        var data = JSON.parse(buffer.toString());
                        data.key = meta.key;
                        callback(null, data);
                    } catch (err) {
                        callback(err);
                    }
                });
            }
            getNext();
        } catch (err) {
            callback(err);
        }
    }

    loadDatabase();

    var lock = 0;
    function checkQueue() {
        lock++;
        if (lock > 1 || writeQueue.length === 0) { return; }

        // Pre-build the buffers for the lines
        var totalSize = 0;
        var i, length = writeQueue.length;
        for (i = 0; i < length; i++) {
            var item = writeQueue[i];
            var line = new Buffer(JSON.stringify(item.meta) + "\t" + JSON.stringify(item.doc) + "\n");
            item.line = line;
            totalSize += line.length;
        }

        // Merge the line buffers into a single large buffer
        var buffer = new Buffer(totalSize);
        var offset = 0;
        for (i = 0; i < length; i++) {
            var item = writeQueue[i];
            var line = item.line;
            line.copy(buffer, offset);
            offset += line.length;
        }

        // Move the queue to a local variable.
        var items = writeQueue;
        writeQueue = [];

        fs.write(writer, buffer, null, buffer.length, null, function (err, written) {
            if (err) {
                // Pass errors through to the callback if there is one
                for (i = 0; i < length; i++) {
                    var item = items[i];
                    if (item.callback) {
                        item.callback(err);
                    } else {
                        // Crash otherwise
                        throw err;
                    }
                }
                return;
            }
            for (i = 0; i < length; i++) {
                var item = items[i];
                if (item.callback) {
                    item.callback(null, item.meta);
                }
            }
            lock = 0;
            checkQueue();
        });

    }

    return {
        get length() {
            return Object.keys(index).length;
        },

        // Saves a document with optional key. The effect if immediate to the
        // running program, but not persistent till after the callback.
        // Pass null as the key to get a generated key.
        save: function (key, doc, callback) {
            if (!key) {
                key = maxKey = maxKey + 1;
            }
            index[key] = {key: key, doc:doc};
            writeQueue.push({meta: {key: key}, doc: doc, callback: callback});
            checkQueue();
        },

        // Removes a document from the collection by key
        // The effect is immediate to the running program, but not permanent
        // till the callback returns.
        remove: function (key, callback) {
            if (key in index) {
                delete index[key];
                writeQueue.push({meta: {key: key, deleted: true}, doc: {}, callback: callback});
            }
        },

        // Returns a readable stream of the whole collection.
        // Supports pause and resume so that you can delay events for layer.
        // This queues "data" and "end" events in memory./
        // Also you can provide a filter to pre-filter results before they
        // go to the queue
        stream: function (filter) {
            var counter = 0;
            var stream = new process.EventEmitter();
            var buffer = [];
            var paused = false;

            // Checks to see if we should emit the "end" event yet.
            function checkDone() {
                if (!paused && counter === 0) {
                    counter--;
                    stream.emit("end");
                }
            }

            // Tries to push events through
            function flush() {
                if (paused) { return; }
                for (var i = 0, l = buffer.length; i < l; i++) {
                    stream.emit("data", buffer[i]);
                    counter--;
                }
                buffer.length = 0;
                process.nextTick(checkDone);
            }


            stream.pause = function () {
                paused = true;
            };

            // Resumes emitting of events
            stream.resume = function () {
                paused = false;
                process.nextTick(function () {
                    flush();
                    checkDone();
                });
            };

            Object.keys(index).forEach(function (key) {
                counter++;
                getByKey(key, function (err, doc) {
                    if (err) {
                        stream.emit("error", err);
                        return;
                    }
                    if (!filter || filter(doc)) {
                        buffer.push(doc);
                        flush();
                    } else {
                        counter--;
                        process.nextTick(checkDone);
                    }
                });
            });

            process.nextTick(checkDone);

            return stream;
        },

        // Loads a single document by id
        get: getByKey
    };
}

module.exports = nStore;