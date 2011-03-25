// Copyright 2010 Tim Caswell <tim@creationix.com>
//
// MIT licensed


var sys = require('sys'),
  fs = require('fs'),
  Path = require('path'),
  Buffer = require('buffer').Buffer;

// This size only affects performance, it's not a constraint on data sizes
var CHUNK_SIZE = 1024;
// This is the max size of a single serialized document
var MAX_SIZE = 1024 * 1024;

// Reads from a given file descriptor at a specified position and length
// Handles all OS level chunking for you.
// Callback gets (err, buffer)
function fsRead(fd, position, length, callback) {
  var buffer = new Buffer(length),
      offset = 0;

  function readChunk() {
    fs.read(fd, buffer, offset, length - offset, position, function (err, bytesRead) {
      if (err) { callback(err); return; }

      offset += bytesRead;

      if (offset < length) {
        readChunk();
        return;
      }
      callback(null, buffer);
    });
  }
  readChunk();
}

// Writes a buffer to a specified file descriptor at the given offset
// handles chunking for you.
// Callback gets (err)
function fsWrite(fd, buffer, position, callback) {
  var offset = 0,
      length = buffer.length;

  function writeChunk() {
    fs.write(fd, buffer, offset, length - offset, position, function (err, bytesWritten) {
      if (err) { callback(err); return; }
      offset += bytesWritten;
      if (offset < length) {
        writeChunk();
        return;
      }
      callback();
    });
  }
  writeChunk();
}


function nStore(filename, filterFn, isTemp) {
  var fd, // FD for reading and writing to the file
      index = {}, // Index of file positions of all documents by key
      writeQueue = [], // Queue of new docs to write to the hd
      stale = 0,
      dbLength = 0, // The size of the current db file in bytes
      compacting = false,
      lastCompact = Date.now();

  // Open a single handle for reading and writing
  fd = fs.openSync(filename, "a+");

  // Generates a random unique 16 char base 36 string
  // (about 2^83 possible keys)
  function makeUUID() {
    var key = "";
    while (key.length < 16) {
      key += Math.floor(Math.random() * 0x290d7410000).toString(36);
    }
    key = key.substr(0, 16);
    if (key in index) {
      return makeUUID();
    }
    return key;
  }

  // Load a single record from the disk
  function getByKey(key, callback) {
    try {
      var info = index[key];
      if (!info) {
        var error = new Error("Document does not exist for " + key);
        error.errno = process.ENOENT;
        callback(error);
        return;
      }

      fsRead(fd, info.position, info.length, function (err, buffer) {
        if (err) { callback(err); return; }
        try {
          var data = JSON.parse(buffer.toString());
          callback(null, data, info.meta);
        } catch (err) {
          callback(err);
        }
      });
    } catch (err) {
      callback(err);
    }
  }



  function compact() {
    // Don't run if already clean or already compacting
    if (isTemp || compacting || stale === 0) { return; }
    compacting = true;
    var tmpFile = Path.join(Path.dirname(filename), makeUUID() + ".tmpdb"),
        tmpDb = nStore(tmpFile, null, true),
        keys = Object.keys(index),
        counter = keys.length;

    keys.forEach(function (key) {
      getByKey(key, function (err, doc, meta) {
        if (err) { throw err; }

        function check() {
          counter--;
          if (counter === 0) {
            done();
          }
        }

        // Hook to allow filtering when compacting
        // Great for things like session pruning
        if (filterFn && !filterFn(doc, meta)) {
          check();
          return;
        }

        tmpDb.save(key, doc, function (err, meta) {
          if (err) {
            throw err;
          }
          check();
        });
      });
    });
    stale = 0;

    function done() {

      // Swap out stores
      var oldfd = fd;
      fd = tmpDb.fd;
      dbLength = tmpDb.dbLength;
      index = tmpDb.index;

      // And clean up the files
      fs.close(oldfd, function (err) {
        if (err) throw err;
        fs.unlink(filename, function (err) {
          if (err) throw err;
          fs.rename(tmpFile, filename, function (err) {
            if (err) throw err;
            compacting = false;
            lastCompact = Date.now();
            checkQueue();
          });
        });
      });
    }

  }

  // Loads the database from disk using blocking I/O
  // TODO: see if non-blocking is faster, this takes a long time
  function loadDatabase() {

    // Create a buffer for reading chunks from the disk
    var chunk = new Buffer(CHUNK_SIZE);

    // Create an empty stream buffer
    var input = new Buffer(MAX_SIZE);
    var input_length = 0;

    // These are positions in the database file
    var offset = 0;
    var base = 0;

    // This is a position within the input stream
    var pos = 0;
    var mid = 0;

    // Read a chunk from the file into `chunk`
    while ((chunk.length = fs.readSync(fd, chunk, 0, CHUNK_SIZE, offset)) > 0) {

      // Move the offset so the outer loop stays in sync
      offset += chunk.length;

      // Copy the chunk onto the input stream
      chunk.copy(input, input_length, 0, chunk.length);
      input_length += chunk.length;

      // See if there is input to consume
      for (var i = pos, l = input_length; i < l; i++) {
        if (input[i] === 9) {
          mid = i + 1;
        }
        if (mid && input[i] === 10) {
          // var doc = input.slice(pos, mid - 1).toString();
          var meta = JSON.parse(input.slice(mid, i).toString());
          var info = {
            meta: meta,
            position: base + pos,
            length: mid - pos - 1
          };
          if (index[meta.key]) {
            stale++;
          }
          if (info.length > 0) {
            index[meta.key] = info;
          } else {
            delete index[meta.key];
          }
          mid = 0;
          pos = i + 1;
        }
      }

      // Shift the input back down
      if (pos > 0) {
        input.copy(input, 0, pos, input.length);
        input_length -= pos;
        base += pos;
        pos = 0;
      }
    }

    dbLength = offset;

  }
  loadDatabase();
  compact();

  var lock = false;
  function checkQueue() {
    if (compacting || lock || writeQueue.length === 0) { return; }
    lock = true;

    // Pull some jobs off the writeQueue
    var length = writeQueue.length;
    var i = 0;
    var size = 0;
    var toWrite = [];
    var newIndex = {};
    var position = dbLength;
    while (i < length && size < 50000) {
      var item = writeQueue[i];
      var data = item.doc ? JSON.stringify(item.doc) : "";
      var key = item.key;
      var meta = {key: key};
      var line = new Buffer(data + "\t" + JSON.stringify(meta) + "\n");
      var dataLength = Buffer.byteLength(data);
      // Generate a callback closure
      toWrite[toWrite.length] = {
        line: line,
        key: key,
        callback: item.callback
      };
      newIndex[meta.key] = {
        position: dbLength,
        length: dataLength,
        meta: meta
      };

      dbLength += line.length;
      size += line.length;
      i++;
    }
    length = i;
    writeQueue.splice(0, length);

    // Merge the buffers into one large one
    var offset = 0;
    var buffer = new Buffer(size);
    for (var i = 0; i < length; i++) {
      var line = toWrite[i].line;
      line.copy(buffer, offset);
      offset += line.length;
    }

    fsWrite(fd, buffer, position, function (err) {
      if (err) {
        throw err;
      }

      // Mix in the updated indexes
      var willCompact = false;
      var threshold = Object.keys(index).length;
      Object.keys(newIndex).forEach(function (key) {
        if (index[key]) {
          stale++;
          if (stale > threshold) {
            willCompact = true;
          }
        }

        if (newIndex[key].length === 0) {
          delete index[key];
        } else {
          index[key] = newIndex[key];
        }
      });

      // Call all the individual callbacks for the write
      for (var i = 0; i < length; i++) {
        var item = toWrite[i];
        var callback = item.callback;
        if (callback) {
          callback(err, {key: item.key});
        }
        
      }

      // Unlock and try the loop again
      lock = false;
      if (willCompact && (Date.now() - lastCompact > 2000)) {
        compact();
      } else {
        process.nextTick(checkQueue);
      }
    });

  }

  function getStream(filter) {
    var counter = 0;
    var stream = new process.EventEmitter();
    var queue = [];
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
      for (var i = 0, l = queue.length; i < l; i++) {
        var item = queue[i];
        stream.emit("data", item.doc, item.meta);
        counter--;
      }
      queue.length = 0;
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
      getByKey(key, function (err, doc, meta) {
        if (err) {
          stream.emit("error", err);
          return;
        }
        if (!filter || filter(doc, meta)) {
          queue.push({
            doc: doc,
            meta: meta
          });
          flush();
        } else {
          counter--;
          process.nextTick(checkDone);
        }
      });
    });

    process.nextTick(checkDone);

    return stream;
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
        key = makeUUID();
      }
      writeQueue[writeQueue.length] = {
        key: key,
        doc: doc,
        callback: callback
      };
      checkQueue();
    },

    // Removes a document from the collection by key
    // The effect is immediate to the running program, but not permanent
    // till the callback returns.
    remove: function (key, callback) {
      if (key in index) {
        delete index[key];
        var line = new Buffer("\t" + JSON.stringify({key: key}) + "\n");

        writeQueue[writeQueue.length] = {
          meta: {key: key},
          position: dbLength,
          length: 0,
          line: line,
          callback: callback
        };
        dbLength += line.length;
        checkQueue();
      } else {
          var err = new Error("Cannot delete a document that does not exist");
          err.errno = process.ENOENT;
          callback(err);
      }
    },

    all: function (filter, callback) {
      if (typeof filter === 'function' && callback === undefined) {
        callback = filter;
        filter = null;
      }
      var docs = [];
      var metas = [];
      var stream = getStream(filter);
      stream.addListener('data', function (doc, meta) {
        docs.push(doc);
        metas.push(meta);
      });
      stream.addListener('end', function () {
        callback(null, docs, metas);
      });
      stream.addListener('error', callback);
    },

    // Returns a readable stream of the whole collection.
    // Supports pause and resume so that you can delay events for layer.
    // This queues "data" and "end" events in memory./
    // Also you can provide a filter to pre-filter results before they
    // go to the queue
    stream: getStream,

    // Loads a single document by id, accepts key and callback
    // the callback will be called with (err, doc, meta)
    get: getByKey,


    // Removes all documents from a database
    clear: function () {
      index = {};
      compact();
    },

    compact: compact,

    // Expose some private variables
    get index() { return index; },
    get fd() { return fd; },
    get dbLength() { return dbLength; },

    // Expose the UUID maker
    makeUUID: makeUUID
  };
}

module.exports = nStore;

