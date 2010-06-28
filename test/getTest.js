require('./helper');

var users = nStore('fixtures/sample.db');

expect("get");
users.get("creationix", function (err, doc, meta) {
  fulfill("get");
  if (err) throw err;
  assert.deepEqual(doc, {name:"Tim Caswell",age:28}, "Document loaded");
  assert.deepEqual(meta, {key:"creationix",created:1277765030789}, "Meta Loaded");
});

expect("get missing");
users.get("fake_key", function (err, doc, meta) {
  fulfill("get missing");
  assert.ok(err instanceof Error, "error is an Error");
  assert.equal(err.errno, process.ENOENT, "Error instance should be ENOENT");
  assert.ok(!doc, "no doc loaded");
  assert.ok(!meta, "no meta loaded");
});
