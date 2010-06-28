
var sys = global.sys = require('sys');
global.assert = require('assert');
global.nStore = require('../lib/nstore');
global.p = function () {
  sys.error(sys.inspect.apply(sys, arguments));
};