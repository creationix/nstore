// This is my proto library but without changing Object.prototype
// Then only sub-objects of Class have the special properties.
var Class = module.exports = Object.create(Object.prototype, {
  // Implements a forEach much like the one for Array.prototype.forEach, but for
  // any object.
  forEach: {value: function forEach(callback, thisObject) {
    var keys = Object.keys(this);
    var length = keys.length;
    for (var i = 0; i < length; i++) {
      var key = keys[i];
      callback.call(thisObject, this[key], key, this);
    }
  }},
  // Implements a map much like the one for Array.prototype.map, but for any
  // object. Returns an array, not a generic object.
  map: {value: function map(callback, thisObject) {
    var accum = [];
    var keys = Object.keys(this);
    var length = keys.length;
    for (var i = 0; i < length; i++) {
      var key = keys[i];
      accum[i] = callback.call(thisObject, this[key], key, this);
    }
    return accum;
  }},
  // Implement extend for easy prototypal inheritance
  extend: {value: function extend(obj) {
    obj.__proto__ = this;
    return obj;
  }},
  // Implement new for easy self-initializing objects
  new: {value: function () {
    var obj = Object.create(this);
    if (obj.initialize) obj.initialize.apply(obj, arguments);
    // Lock the object down
    Object.seal(obj);
    return obj;
  }}
});
