var sys = require('sys'),
    Collection = require('./lib/agregar'),
    Step = require('step');

sys.puts("Loading database...");
var users = new Collection("data/users.db");
sys.puts("Done loading");

Step(
    function () {
        var num = 50000;
        sys.puts("Inserting " + num + " documents");
        var group = this.group();
        for (var i = 0; i < num; i++) {
            users.save(null, {name: "Tim"+i,age:28}, group());
        }
    },
    function (err, results) {
        sys.puts("Done Inserting");
        sys.puts("Retrieving all documents...");
        var data = [];
        var all = users.stream(function (doc) {
            return parseInt(doc.key, 10) % 2 === 0;
        });
        all.addListener('data', function (doc) {
            data.push(doc);
        });

        all.addListener('end', function () {
            sys.puts("DONE! Loaded " + data.length + " documents.");
        });
        
    }
);

