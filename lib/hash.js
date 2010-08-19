
// A Hash is an interable object
var Hash = module.exports = Object.create(Object.prototype, {
  // Make for easy converting objects to hashes.
  new: {value: function (value) {
    if (value === undefined) return Object.create(Hash);
    value.__proto__ = Hash;
    return value;
  }},
  // Implements a forEach much like the one for Array.prototype.forEach.
  forEach: {value: function forEach(callback, thisObject) {
    var keys = Object.keys(this),
        length = keys.length;
    for (var i = 0; i < length; i++) {
      var key = keys[i];
      callback.call(thisObject, this[key], key, this);
    }
  }},
  // Implements a map much like the one for Array.prototype.map.
  // Returns a normal Array instance.
  map: {value: function map(callback, thisObject) {
    var keys = Object.keys(this),
        length = keys.length,
        accum = new Array(length);
    for (var i = 0; i < length; i++) {
      var key = keys[i];
      accum[i] = callback.call(thisObject, this[key], key, this);
    }
    return accum;
  }},
  length: {get: function length() {
    return Object.keys(this).length;
  }}
});
Object.freeze(Hash);
