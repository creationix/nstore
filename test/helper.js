
global.sys = require('sys');
global.assert = require('assert');
global.fs = require('fs');
global.nStore = require('../lib/nstore');
global.p = function () {
  sys.error(sys.inspect.apply(sys, arguments));
};

// A mini expectations module to ensure expected callback fire at all.
var expectations = {};
global.expect = function expect(message) {
  expectations[message] = new Error("Missing expectation: " + message);
}
global.fulfill = function fulfill(message) {
  delete expectations[message];
}
process.addListener('exit', function () {
  Object.keys(expectations).forEach(function (message) {
    throw expectations[message];
  });
});


function clean() {
  fs.writeFileSync("fixtures/toDelete.db", fs.readFileSync("fixtures/sample.db", "binary"), "binary");
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