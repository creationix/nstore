// This is my proto library but without changing Object.prototype
// Then only sub-objects of Class have the special properties.
var Class = module.exports = Object.create(Object.prototype, {
  // Implement extend for easy prototypal inheritance
  extend: {value: function extend(obj) {
    obj.__proto__ = this;
    // Lock the object to be used only as a prototype
    Object.freeze(obj);
    return obj;
  }},
  // Implement new for easy self-initializing objects
  new: {value: function () {
    var obj = Object.create(this);
    if (obj.initialize) obj.initialize.apply(obj, arguments);
    // Lock the object down so the fields are static
    Object.seal(obj);
    return obj;
  }}
});
