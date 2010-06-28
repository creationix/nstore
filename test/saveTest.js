require('./helper');

var documents = nStore('fixtures/new.db');

var thesis = {
  title: "Node is cool",
  author: "creationix",
  reasons: ["Non-Blocking I/O", "Super Fast", "Powered by bacon"]
};

expect("save");
documents.save("thesis", thesis, function (err) {
  fulfill("save");
  if (err) throw err;
  assert.equal(documents.length, 1, "There should be 1 document in the collection");
  expect("get");
  documents.get("thesis", function (err, doc, meta) {
    fulfill("get");
    if (err) throw err;
    p(arguments);
    assert.deepEqual(doc, thesis, "Loading it back should look the same");
    assert.equal(meta.key, "thesis", "The meta should have the key");
  });
});
