var EventEmitter = require('events').EventEmitter,
    Path = require('path'),
    fs = require('fs'),
    Step = require('step'),
    Class = require('./class'),
    Queue = require('./queue'),
    File = require('./file'),
    makeUUID = require('./uuid');

const CHUNK_LENGTH = 40 * 1024,
      TAB = 9,
      NEWLINE = 10;

var nStore = module.exports = Class.extend({

  // Set up out local properties on the object
  // and load the datafile.
  initialize: function initialize(filename, callback) {
    if (this === nStore) throw new Error("Can't call initialize directly");
    this.filename = filename;
    this.fd = null;
    this.index = {};
    this.writeQueue = Queue.new();
    this.stale = 0;
    this.dbLength = 0;
    this.busy = false;
    this.filterFn = null;
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
          process.nextTick(function () {
            if (typeof callback === 'function') callback(null, self);
            self.checkQueue();
          });
        }
      }

      function emit(line) {
        counter++;
        File.read(fd, line[0], line[1] - line[0], function (err, key) {
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
        tmpDb;

    this.busy = true;
    var self = this;
    Step(
      function makeNewDb() {
        tmpDb = nStore.new(tmpFile, this);
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

      File.read(this.fd, info.position, info.length, function (err, buffer) {
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
    var next = this.writeQueue.shift();
    if (next === undefined) {
      // Compact when the db is over half stale
      if (this.stale > (this.length - this.stale)) {
        this.compactDatabase();
      }
      return;
    }
    this.busy = true;
    var self = this;
    try {
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
        File.write(self.fd, line, self.dbLength, this);
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

});

