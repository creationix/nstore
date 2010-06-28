var sys = require('sys'),
    Collection = require('./lib/agregar'),
    Step = require('step');

checkRam("Startup");
var users = new Collection("data/users.db");
checkRam("After loading db with " + users.length + " documents");

function checkRam(message) {
    sys.puts(message + ":\n" + sys.inspect(process.memoryUsage()));
}

var num = 1000000;
var left = num;
function insert() {
    if (left > 0) {
        var counter = 1000;
        left -= counter;
        for (var i = 0; i < counter; i++) {
            users.save(1, {hello: "world"}, function () {
                counter--;
                if (counter === 0) {
                    insert();
                }
            });
        }
    } else {
        checkRam("After queueing " + num + " inserts");
    }
}
insert();

process.addListener("exit", function () {
    checkRam("On Exit");
})

// Step(
//     function () {
//         var num = 10;
//         sys.puts("Inserting " + num + " documents");
//         var group = this.group();
//         for (var i = 0; i < num; i++) {
//             users.save(null, {name: "Tim"+i,age:28}, group());
//         }
//     },
//     function (err, results) {
//         sys.puts("Done Inserting");
//         sys.puts("Retrieving all documents...");
//         var data = [];
//         var all = users.stream();
//         all.addListener('data', function (doc) {
//             data.push(doc);
//         });
// 
//         all.addListener('end', function () {
//             sys.puts("DONE! Loaded " + data.length + " documents.");
//         });
//         
//     }
// );
// 
