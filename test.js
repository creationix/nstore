var sys = require('sys'),
    nStore = require('./lib/nstore'),
    Step = require('step');

checkRam("Startup");
var users = new nStore("data/users.db");
checkRam("After loading db with " + users.length + " documents");

function checkRam(message) {
    // sys.puts(message + ":\n" + sys.inspect(process.memoryUsage()));
}
function p(val) {
    sys.error(sys.inspect(val));
}

users.save(1, {name:"Tim Caswell",age:28}, function (err) {
    if (err) throw err;
    sys.debug("Saved");
    users.get(1, function (err, doc, meta) {
        if (err) throw err;
        sys.debug("Loaded");
        p([doc, meta]);
        users.remove(1, function (err) {
            if (err) throw err;
            sys.debug("Deleted");
            
        });
    });
});

// users.get(2, p);
// 
// users.save(1, {hello:"World1"});
// users.save(2, {hello:"World2"});
// users.save(3, {hello:"World3"});

// var num = 1000000;
// var left = num;
// function insert() {
//     if (left > 0) {
//         var counter = 1000;
//         left -= counter;
//         for (var i = 0; i < counter; i++) {
//             users.save(i, {hello: "world"}, function () {
//                 counter--;
//                 if (counter === 0) {
//                     insert();
//                 }
//             });
//         }
//     } else {
//         checkRam("After queueing " + num + " inserts");
//     }
// }
// insert();

process.addListener("exit", function () {
    checkRam("On Exit");
});
// 
// // Step(
// //     function () {
// //         var num = 10;
// //         sys.puts("Inserting " + num + " documents");
// //         var group = this.group();
// //         for (var i = 0; i < num; i++) {
// //             users.save(null, {name: "Tim"+i,age:28}, group());
// //         }
// //     },
// //     function (err, results) {
// //         sys.puts("Done Inserting");
// //         sys.puts("Retrieving all documents...");
// //         var data = [];
// //         var all = users.stream();
// //         all.addListener('data', function (doc) {
// //             data.push(doc);
// //         });
// // 
// //         all.addListener('end', function () {
// //             sys.puts("DONE! Loaded " + data.length + " documents.");
// //         });
// //         
// //     }
// // );
// // 
