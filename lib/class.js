// This is my proto library but without changing Object.prototype
// Then only sub-objects of Class have the special properties.
var Class = module.exports = Object.create(Object.prototype, {
  // Implement extend for easy prototypal inheritance
  extend: {value: function extend(obj) {
    if (obj === undefined) return Object.create(this);
    // Hook back so the constructor function still works like classical if needed
    if (typeof obj.constructor === 'function') {
      Object.defineProperty(obj.constructor, "prototype", {value: obj});
    }
    obj.__proto__ = this;
    Object.freeze(obj); // Lock the prototype to enforce no changes
    return obj;
  }},

  // Implement new for easy self-initializing objects
  new: {value: function new_() {
    var obj = Object.create(this);
    if (typeof obj.constructor === 'function') obj.constructor.apply(obj, arguments);
    Object.seal(obj); // Lock the object down so the fields are static
    return obj;
  }}

});

