// This plugin adds an auto-expiring ramCache to nStore
module.exports = function CachePlugin() {
  var values = [],
      items = 0;
  
  var self = {
    save: function save(key, value, callback) {
      if (!values.hasOwnProperty(key)) {
        items++;
        if (items > maxSize) {
          delete values[Object.keys(values)[0]];
          items--;
        }
      }
      values[key] = value;
      return dataSource.save(key, value, callback);
    },
    get: function get(key, callback) {
      if (values.hasOwnProperty(key)) {
        process.nextTick(function () {
          callback(null, values[key], key);
        });
        return;
      }
      dataSource.get(key, callback);
    },
    remove: function remove(key, callback) {
      delete values[key];
      items--;
      return dataSource.remove(key, callback);
    }
  };
  return self;
}

