// Copyright 2010 Tim Caswell <tim@sencha.com>


var sys = require('sys'),
    fs = require('fs'),
    Buffer = require('buffer').Buffer;

// This size only affects performance, it's not a constraint on data sizes
var CHUNK_SIZE = 1024;
// This is the max size of a single serialized document
var MAX_SIZE = 1024 * 1024;


function Collection(filename) {
    var writer, reader; // FDs for reading and writing to the file
    var index = {};
    var writeQueue = [];
    var maxKey = 0;

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
        var next = writeQueue.shift();
        var entry = new Buffer(JSON.stringify(next.meta) + "\t" + JSON.stringify(next.doc) + "\n");
        fs.write(writer, entry, null, entry.length, null, function (err, written) {
            if (err) {
                if (next.callback) {
                    next.callback(err);
                    return;
                }
                throw err;
            }
            if (next.callback) {
                next.callback(null, next.doc);
            }
            lock = 0;
            checkQueue();
        });

    }

    return {
        // Saves a document with optional key. The effect if immediate to the
        // running program, but not persistent till after the callback.
        // Pass null as the key to get a generated key.
        save: function (key, doc, callback) {
            if (!key) {
                key = maxKey = maxKey + 1;
            }

            writeQueue.push({meta: {key: key}, doc: doc, callback: callback});
            checkQueue();
        },

        // Removes a document from the collection by key
        // The effect is immediate to the running program, but not permanent
        // till the callback returns.
        remove: function (key, callback) {
            if (key in index) {
                delete index[key];
                writeQueue.push({callback: callback, meta: {key: key, deleted: true}, doc: {}});
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

module.exports = Collection;