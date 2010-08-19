var Class = require('./class');

// This global modifier allows you to treat constructor based classes as if
// they were prototype based.

// Define an extend method for constructor functions that works like prototype extends.
Object.defineProperty(Function.prototype, "extend", {value: function extend(obj) {
  // Clone the functions's prototype
  var props = {}, proto = this.prototype;
  Object.getOwnPropertyNames(proto).forEach(function (key) {
    props[key] = Object.getOwnPropertyDescriptor(proto, key);
  });
  
  // Put the constructor on a prop too
  props.constructor = {value: this};
  
  // Make obj's parent be an Classy version of props
  if (obj === undefined) return Object.create(Class, props);
  obj.__proto__ = Object.create(Class, props);
  Object.freeze(obj);
  return obj;
}});

// This is the "new" keyword implemented in pure ES5
// I added sealing on top of what "new" does
Object.defineProperty(Function.prototype, "new", {value: function () {
  var obj = Object.create(this.prototype, {constructor: this});
  var result = this.apply(obj, arguments);
  Object.seal(obj);
  return result === undefined ? obj : result;
}});
