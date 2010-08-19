var Class = require('./class');

// If a large number of writes gets queued up, the shift call normally
// eats all the CPU.  This implementes a faster queue.
var Queue = module.exports = Class.extend({
  initialize: function initialize() {
    if (this === Queue) throw new Error("Cann't use Queue directly");
    this.tail = [];
    this.head = Array.prototype.slice.call(arguments);
    this.offset = 0;
    // Lock the object down
    Object.seal(this);
    return this;
  },
  shift: function shift() {
    if (this.offset === this.head.length) {
      var tmp = this.head;
      tmp.length = 0;
      this.head = this.tail;
      this.tail = tmp;
      this.offset = 0;
      if (this.head.length === 0) return;
    }
    return this.head[this.offset++];
  },
  push: function push(item) {
    return this.tail.push(item);
  },
  get length() {
    return this.head.length - this.offset + this.tail.length;
  }
});
