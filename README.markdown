# nStore

A simple in-process document store for node.js

## Setup

All the examples assume this basic setup

    // Load the library
    var nStore = require('nstore');
    // Create a store
    var users = nStore('data/users.db');


## Creating a database

Creating a database is easy, you just call the nStore function to generate a collection object.  Then call save to insert/update documents

    // Insert a new document with key "creationix"
    users.save("creationix", {name: "Tim Caswell": age: 28}, function (err) {
        // The save is finished and written to disk safely
    });

## Loading a database

Assuming the previous code was run, a file will now exist with the persistent data inside it.

    // Insert a new document with key "creationix"
    users.get("creationix", function (err, doc) {
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

This is a full stream interface complete with `pause` and `resume`.  Any rows that are read from the disk while it's paused will be queued internally, but will call the pre-filter function right away so it doesn't buffer results we don't want to keep.

## Removing a document

Remove is by key only.

    // Remove our new document
    users.remove(1, function (err) {
        // The document at key 1 was removed
    });
