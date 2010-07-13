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
  var buffer = new Buffer(length);
  var offset = 0;
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
  var offset = 0;
  var length = buffer.length;
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


function nStore(filename, filterFn) {
  var fd; // FD for reading and writing to the file
  var index = {}; // Index of file positions of all documents by key
  var writeQueue = []; // Queue of new docs to write to the hd
  var queueSize = 0;
  var stale = 0;
  var dbLength = 0; // The size of the current db file in bytes

  // Open a single handle for reading and writing
  fd = fs.openSync(filename, "a+");

  var compactQueue = false;
  var compacting = false;
  function compact() {
    // Don't run if already clean or already compacting
    if (compacting || stale === 0) { return; }
    compacting = true;
    compactQueue = [];
    var tmpFile = Path.join(Path.dirname(filename), makeUUID() + ".tmpdb");
    var tmpDb = nStore(tmpFile);
    var keys = Object.keys(index);
    var counter = keys.length;
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
      if (compactQueue.length > 0) {
        var counter = compactQueue.length;
        compactQueue.forEach(function (pair) {
          tmpDb.save(pair[0], pair[1], function (err, meta) {
            if (err) throw err;
            counter--;
            if (counter === 0) {
              done();
            }
          });
        });
        compactQueue = false;
        return;
      }
      compactQueue = false;

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
          });
        });
      });
    }

  }

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

  // Loads the database from disk using blocking I/O
  // TODO: see if non-blocking is faster, this takes a long time
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
    while ((chunk.length = fs.readSync(fd, chunk, 0, CHUNK_SIZE, offset)) > 0) {

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
        input.length -= pos;
        base += pos;
        pos = 0;
      }
    }

    dbLength = offset;

  }
  loadDatabase();
  compact();

  // Load a single record from the disk
  function getByKey(key, callback) {
    try {
      var info = index[key];
      if (!info) {
        var err = new Error("Document does not exist for " + key);
        err.errno = process.ENOENT;
        callback(err);
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

  var lock = 0;
  function checkQueue() {
    if (writeQueue.length === 0) { return; }
    lock++;
    if (lock > 1) { return; }

    // Merge the line buffers into a single large buffer
    var buffer = new Buffer(queueSize);
    var offset = 0;
    var position;
    var length = writeQueue.length;
    var callbacks = [];
    var newIndex = {};
    for (var i = 0; i < length; i++) {
      // Unpack the queue item
      var item = writeQueue[i],
          line = item.line,
          meta = item.meta,
          key = meta.key;

      position = position || item.position;
      callbacks[i] = item.callback;
      newIndex[key] = {
        position: item.position,
        length: item.length,
        meta: meta
      };

      // Copy the line into the larger buffer
      line.copy(buffer, offset);
      offset += line.length;
    }

    // Empty the queue
    writeQueue.length = 0;
    queueSize = 0;
    fsWrite(fd, buffer, position, function (err) {
      if (err) {
        throw err;
      }
      if (index[key]) {
        stale++;
        if (stale > Object.keys(index).length / 2) {
          process.nextTick(compact);
        }
      }
      // Mix in the updated indexes
      Object.keys(newIndex).forEach(function (key) {
        if (newIndex[key].length === 0) {
          delete index[key];
        } else {
          index[key] = newIndex[key];
        }
      });

      // Call all the individual callbacks for the write
      for (var i = 0; i < length; i++) {
        var fn = callbacks[i];
        if (fn) {
          fn(err);
        } else {
          if (err) {
            throw err;
          }
        }
      }

      // Unlock and try the loop again
      lock = 0;
      process.nextTick(checkQueue);
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
      var data = JSON.stringify(doc);
      var line = new Buffer(data + "\t" + JSON.stringify({key: key}) + "\n");
      var meta = {key: key};
      if (compactQueue) {
        compactQueue.push([key, doc]);
      }
      writeQueue[writeQueue.length] = {
        meta: meta,
        position: dbLength,
        length: Buffer.byteLength(data),
        line: line,
        callback: function (err) {
          callback && callback(err, meta);
        }
      };
      dbLength += line.length;
      queueSize += line.length;
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
        queueSize += line.length;
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

    // Expose some private variables
    get index() { return index; },
    get fd() { return fd; },
    get dbLength() { return dbLength; },

    // Expose the UUID maker
    makeUUID: makeUUID
  };
}

module.exports = nStore;

