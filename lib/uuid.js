// Generates a random unique 16 char base 36 string
// (about 2^83 possible keys)
module.exports = function makeUUID(index) {
  var key = "";
  while (key.length < 16) {
    key += Math.floor(Math.random() * 0xcfd41b9100000).toString(36);
  }
  key = key.substr(0, 16);
  if (index.hasOwnProperty(key)) {
    return makeUUID(index);
  }
  return key;
};
