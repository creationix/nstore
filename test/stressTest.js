require('./helper');

console.log(sys.inspect(process.memoryUsage()));

var documents = nStore('fixtures/new.db');
var counter = 0;

const NUM = 1000000;
for (var i = 0; i < NUM; i++) {
  documents.save((i % 1000) + 1, {i:i}, function () {
    counter++;
    if (counter === NUM) {
      documents.compact();
    }
    
  });
}

process.on('exit', function () {
  console.log(sys.inspect(process.memoryUsage()));
});