var EventEmitter = require('events').EventEmitter,
    Buffer = require('buffer').Buffer,
    Path = require('path'),
    Step = require('step'),
    fs = require('fs');

const CHUNK_LENGTH = 40 * 1024,
      TAB = 9,
      NEWLINE = 10;

var nStore = {

  // Set up out local properties on the object
  // and load the datafile.
  initialize: function initialize(filename, callback) {
    this.filename = filename;
    this.fd = null;
    this.index = {};
    this.writeQueue = [];
    this.writeQueue.__proto__ = fastArray;
    this.stale = 0;
    this.dbLength = 0;
    this.busy = false;
    this.lastCompact = null;
    this.filterFn = null;
    // We don't want any other properties on this object that aren't initialized here
    Object.seal(this);
    this.loadDatabase(callback);
  },

  // getter property that returns the number of documents in the database
  get length() {
    return Object.keys(this.index).length;
  },

  loadDatabase: function (callback) {
    var buffer = new Buffer(CHUNK_LENGTH);
    var index = {};
    var scanned = false;
    var self = this;
    var stale = 0;
    var counter = 0;
    this.busy = true;

    fs.open(this.filename, 'a+', 0666, function (err, fd) {
      if (err) { callback(err); return; }
      var line = [0, null, null];
      function readChunk(position) {
        fs.read(fd, buffer, 0, CHUNK_LENGTH, position, function (err, bytes) {
          if (err) throw err;
          if (!bytes) {
            scanned = position;
            check();
            return;
          }
          buffer.length = bytes;
          for (var i = 0; i < bytes; i++) {
            switch (buffer[i]) {
              case TAB:
                line[1] = position + i;
                next = NEWLINE;
                break;
              case NEWLINE:
                line[2] = position + i;
                emit(line);
                line = [position + i + 1, position + i, null];
                break;
            }
          }
          readChunk(position + bytes);
        });
      }
      readChunk(0);

      function check() {
        if (counter === 0 && scanned !== undefined) {
          self.dbLength = scanned;
          self.index = index;
          self.fd = fd;
          self.stale = stale;
          self.busy = false;
          if (typeof callback === 'function') callback();
          self.checkQueue();
        }
      }

      function emit(line) {
        counter++;
        fsRead(fd, line[0], line[1] - line[0], function (err, key) {
          if (index.hasOwnProperty(key)) {
            stale++;
          }
          if (line[2] - line[1] - 1 === 0) {
            delete index[key];
          } else {
            index[key] = {
              position: line[1] + 1,
              length: line[2] - line[1] - 1
            };
          }
          counter--;
          check();
        });

      }
    });
  },

  compactDatabase: function (clear, callback) {
    if ((!clear && this.stale === 0) || this.busy) return;
    var tmpFile = Path.join(Path.dirname(this.filename), makeUUID(this.index) + ".tmpdb"),
        tmpDb = Object.create(nStore);

    this.busy = true;
    var self = this;
    Step(
      function makeNewDb() {
        tmpDb.initialize(tmpFile, this);
      },
      function copyData(err) {
        if (err) throw err;
        if (clear) return true;
        var group = this.group();
        var copy = Step.fn(
          function (key) {
            self.get(key, this);
          },
          function (err, doc, key) {
            if (err) throw err;
            if (self.filterFn && self.filterFn(doc, key)) {
              return true;
            }
            tmpDb.save(key, doc, this);
          }
        );

        Object.keys(self.index).forEach(function (key) {
          copy(key, group());
        });
      },
      function closeOld(err) {
        if (err) throw err;
        fs.close(self.fd, this);
      },
      function moveNew(err) {
        if (err) throw err;
        fs.rename(tmpFile, self.filename, this);
      },
      function transitionState(err) {
        if (err) throw err;
        self.dbLength = tmpDb.dbLength;
        self.index = tmpDb.index;
        self.fd = tmpDb.fd;
        self.stale = tmpDb.stale;
        return true;
      },
      function cleanup(err) {
        self.busy = false;
        process.nextTick(function () {
          self.checkQueue();
        });
        if (err) throw err;
        return true;
      },
      function prologue(err) {
        if (callback) {
          callback(err);
        }
      }
    );

  },

  // Saves a document with optional key. The effect if immediate to the
  // running program, but not persistent till after the callback.
  // Pass null as the key to get a generated key.
  save: function save(key, doc, callback) {
    if (!key) {
      key = makeUUID(this.index);
    }
    this.writeQueue.push({
      key: key.toString(),
      doc: doc,
      callback: callback
    });
    this.checkQueue();
  },

  // Load a single record from the disk
  get: function getByKey(key, callback) {
    try {
      var info = this.index[key];
      if (!info) {
        var error = new Error("Document does not exist for " + key);
        error.errno = process.ENOENT;
        callback(error);
        return;
      }

      fsRead(this.fd, info.position, info.length, function (err, buffer) {
        if (err) { callback(err); return; }
        try {
          var data = JSON.parse(buffer.toString());
          callback(null, data, key);
        } catch (err) {
          callback(err);
        }
      });
    } catch (err) {
      callback(err);
    }
  },

  remove: function removeByKey(key, callback) {
    try {
      var info = this.index[key];
      if (!info) {
        var error = new Error("Document does not exist for " + key);
        error.errno = process.ENOENT;
        callback(error);
        return;
      }
      this.save(key, null, callback);
    } catch(err) {
      callback(err);
    }
  },

  clear: function clearAll(callback) {
    if (this.busy) {
      var self = this;
      process.nextTick(function () {
        self.clear(callback);
      });
      return;
    }
    this.compactDatabase(true, callback);
  },

  // Checks the save queue to see if there is a record to write to disk
  checkQueue: function checkQueue() {
    if (this.busy) return;
    if (this.writeQueue.length === 0) {
      // Compact when the db is over half stale
      if (this.stale > (this.length - this.stale)) {
        this.compactDatabase();
      }
      return;
    }
    this.busy = true;
    var self = this;
    try {
      var next = this.writeQueue.fastShift();
      var line = new Buffer(next.key + "\t" + JSON.stringify(next.doc) + "\n");
      var keyLength = Buffer.byteLength(next.key);
    } catch(err) {
      console.log(err.stack);
      if (next.callback) {
        next.callback(err);
      }
      return;
    }
    Step(
      function writeDocument() {
        fsWrite(self.fd, line, self.dbLength, this);
      },
      function updateIndex(err) {
        if (err) throw err;
        // Count stale records
        if (self.index.hasOwnProperty(next.key)) { self.stale++; }
        if (next.doc) {
          // Update index
          self.index[next.key] = {
            position: self.dbLength + keyLength + 1,
            length: line.length - keyLength - 2
          };
        } else {
          delete self.index[next.key];
        }
        // Update the pointer to the end of the database
        self.dbLength += line.length;
        return true;
      },
      function done(err) {
        self.busy = false;
        if (err) throw err;
        self.checkQueue();
        return true;
      },
      function (err) {
        if (next.callback) {
          next.callback(err, next.key);
        }
      }
    );
  },

};

// Utilities
// Reads from a given file descriptor at a specified position and length
// Handles all OS level chunking for you.
// Callback gets (err, utf8String)
// Reuses a buffer that grows (replaces old one) only when needed
var readBuffer = new Buffer(40*1024);
var readQueue = null;
function fsRead(fd, position, length, callback) {
  if (length > readBuffer.length) {
    readBuffer = new Buffer(length);
    console.log("Upgrading read buffer to " + readBuffer.length);
  }
  var offset = 0;

  function readChunk() {
    fs.read(fd, readBuffer, offset, length - offset, position, function (err, bytesRead) {
      if (err) { callback(err); return; }

      offset += bytesRead;

      if (offset < length) {
        readChunk();
        return;
      }
      callback(null, readBuffer.toString('utf8', 0, length));
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

// Generates a random unique 16 char base 36 string
// (about 2^83 possible keys)
function makeUUID(index) {
  var key = "";
  while (key.length < 16) {
    key += Math.floor(Math.random() * 0xcfd41b9100000).toString(36);
  }
  key = key.substr(0, 16);
  if (index.hasOwnProperty(key)) {
    return makeUUID(index);
  }
  return key;
}

// Makes an async function that takes 3 arguments only execute one at a time.
function safe3(fn) {
  var queue = [];
  var safe = true;
  function checkQueue() {
    var next = queue.shift();
    safe = false;
    fn(next[0], next[1], next[2], function (error, result) {
      next[3](error, result);
      if (queue.length > 0) {
        checkQueue();
      } else {
        safe = true;
      }
    });
  }
  return function (arg1, arg2, arg3, callback) {
    queue.push(arguments);
    if (safe) {
      checkQueue();
    }
  };
}

// If a large number of writes gets queued up, the shift call normally
// eats all the CPU.  This implementes a fast shift for the queue array.
var fastArray = Object.create(Array.prototype, {
  start: {value: 0},
  fastShift: {value: function () {
    var item = this[this.start];
    if (this.start >= Math.floor(this.length / 2)) {
      this.splice(0, this.start + 1);
      this.start = 0;
    } else {
      this.start++;
    }
    return item;
  }}
});


module.exports = nStore;