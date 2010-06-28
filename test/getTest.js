require('./helper');

var users = nStore('fixtures/sample.db');

users.get("creationix", function (err, doc, meta) {
  if (err) throw err;
  assert.deepEqual(doc, {name:"Tim Caswell",age:28}, "Document loaded");
  assert.deepEqual(meta, {key:"creationix",created:1277765030789}, "Meta Loaded");
});

users.get("fake_key", function (err, doc, meta) {
  assert.ok(err instanceof Error, "error is an Error");
  assert.equal(err.errno, process.ENOENT, "Error instance should be ENOENT");
  assert.ok(!doc, "no doc loaded");
  assert.ok(!meta, "no meta loaded");
});
