# nStore

A simple in-process key/value document store for node.js. nStore uses a safe append-only data format for quick inserts, updates, and deletes.  Also a index of all documents and their exact location on the disk is stored in in memory for fast reads of any document.  This append-only file format means that you can do online backups of the datastore using simple tools like rsync.  The file is always in a consistent state.

## Setup

All the examples assume this basic setup.  Currently it reads an existing file if it exists using blocking I/O.  Then means the nStore function is blocking and doesn't need a callback.  The cost of this is that large databases take a long time (relatively) to load.  My test with a 1,000,000 document collection takes about 14 seconds to load.  I may change this api in the future to use the faster non-blocking I/O.

Creating a database is easy, you just call the nStore function to generate a collection object.

    // Load the library
    var nStore = require('nstore');
    // Create a store
    var users = nStore('data/users.db');

## Creating a document

To insert/update documents, just call the save function on the collection.

    // Insert a new document with key "creationix"
    users.save("creationix", {name: "Tim Caswell": age: 28}, function (err) {
        if (err) { throw err; }
        // The save is finished and written to disk safely
    });

    // Or insert with auto key
    users.save(null, {name: "Bob"}, function (err, meta) {
        if (err) { throw err; }
        // You now have the insert id
    });

## Loading a document

Assuming the previous code was run, a file will now exist with the persistent data inside it.

    // Insert a new document with key "creationix"
    users.get("creationix", function (err, doc, meta) {
        if (err) { throw err; }
        // You now have the document
    });

## Streaming an entire collection

Sometimes you want to search a database for certain documents and you don't know all the keys.  In this case, simply create a readable stream from the store and filter the results.

    // Create a stream with a pre-filter on the results
    var userStream = users.stream(function (doc, meta) {
        return doc.age > 18 && doc.age < 40;
    });

    userStream.addListener('data', function (doc, meta) {
        // Do something with the document
    });

    userStream.addListener('end', function () {
        // that's all the results
    });

    userStream.addListener('error', function (err) {
        throw err;
    });

This is a full stream interface complete with `pause` and `resume`.  Any rows that are read from the disk while it's paused will be queued internally, but will call the pre-filter function right away so it doesn't buffer results we don't want to keep.

## Searching for documents

You can search for documents using streams with a filter, but sometimes it's easier to just get the aggregate result after filtering.  For this you can use the `all()` function.

    // Search for several things at once
    users.stream(function (doc, meta) {
        return doc.age > 18 && doc.age < 40;
    }, function (err, docs, metas) {
      if (err) throw err;
      // Do something with the results
    });


## Removing a document

Remove is by key only.

    // Remove our new document
    users.remove("creationix", function (err) {
        if (err) { throw err; }
        // The document at key "creationix" was removed
    });

## Clearing the whole collection

You can also quickly clear the entire collection

    // Clear
    users.clear();

This clears all the keys and triggers a compaction.  Only after the compact finishes is the data truly deleted from the disk, however any further queries cannot see the old data anymore.

## Special compaction filter

There are times that you want to prune stale data from a database, like when using nStore to store session data.  The problem with looping over the index keys and calling `remove()` on them is that it bloats the file. Deletes are actually appends to the file.  Instead nStore exposes a special filter function that, if specified, will filter the data when compacting the data file.

    // Prune any items that have a doc.lastAccess older than 1 hour.
    var session = nStore('data/sessions.db', function (doc, meta) {
      return doc.lastAccess > Date.now() - 360000;
    });

