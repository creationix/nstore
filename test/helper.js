
var sys = global.sys = require('sys');
global.assert = require('assert');
global.fs = require('fs');
global.nStore = require('../lib/nstore');
global.p = function () {
  sys.error(sys.inspect.apply(sys, arguments));
};

function clean() {
  try {
    fs.unlinkSync('fixtures/new.db');
  } catch (err) {
    if (err.errno !== process.ENOENT) {
      throw err;
    }
  }
}

// Clean the test environment at startup
clean();

// Clean on exit too
// process.addListener('exit', clean);