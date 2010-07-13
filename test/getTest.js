require('./helper');

var users = nStore('fixtures/sample.db');

expect("get");
users.get("creationix", function (err, doc, meta) {
  fulfill("get");
  if (err) throw err;
  assert.deepEqual(doc, {name:"Tim Caswell",age:28}, "Document loaded");
  assert.deepEqual(meta, {key:"creationix"}, "Meta Loaded");
});

expect("get2");
users.get("tjholowaychuk", function (err, doc, meta) {
  fulfill("get2");
  if (err) throw err;
  assert.deepEqual(doc, {name:"TJ Holowaychuck",country:"Canada"}, "Document loaded");
  assert.deepEqual(meta, {key:"tjholowaychuk"}, "Meta Loaded");
});

expect("get missing");
users.get("bob", function (err, doc, meta) {
  fulfill("get missing");
  assert.ok(err instanceof Error, "error is an Error");
  if (err.errno !== process.ENOENT) throw err;
  assert.equal(err.errno, process.ENOENT, "Error instance should be ENOENT");
  assert.ok(!doc, "no doc loaded");
  assert.ok(!meta, "no meta loaded");
});
