require('./helper');

console.log(sys.inspect(process.memoryUsage()));

var documents = nStore('fixtures/new.db');
var counter = 0;

const NUM = 100000;
for (var i = 0; i < NUM; i++) {
  documents.save(i, {i:i}, function () {
    counter++;
    if (counter === NUM) {
      documents.compact();
    }
    
  });
}

process.on('exit', function () {
  console.log(sys.inspect(process.memoryUsage()));
});