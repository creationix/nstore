require('./helper');

var users = nStore('fixtures/sample.db');

var stream = users.stream();

expect("data");
stream.addListener("data", function (doc, meta) {
  fulfill("data");
  assert.ok(doc, "Streams documents");
  assert.ok(meta, "Streams meta");
});

expect("end");
stream.addListener("end", function () {
  fulfill("end");
});

expect("filter");
var filteredStream = users.stream(function (doc, meta) {
  fulfill("filter");
  assert.ok(doc, "Streams documents");
  assert.ok(meta, "Streams meta");
  return doc.age === 28;
});

expect("filtered data");
filteredStream.addListener("data", function (doc, meta) {
  fulfill("filtered data");
  assert.deepEqual(doc, {name:"Tim Caswell",age:28}, "Document loaded");
  assert.deepEqual(meta, {key:"creationix"}, "Meta Loaded");
});

expect("filtered end");
filteredStream.addListener("end", function () {
  fulfill("filtered end");
});

// TODO test pause and resume