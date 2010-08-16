var Buffer = require('buffer').Buffer,
    fs = require('fs');

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


module.exports = {
  fsRead: safe3(fsRead),
  fsWrite: fsWrite,
  makeUUID: makeUUID,
  fastArray: fastArray
};