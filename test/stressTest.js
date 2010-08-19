require('./helper');

console.dir(process.memoryUsage());

var counter = 0;
const NUM = 1000000;
var documents = nStore.new('fixtures/new.db', function () {
  for (var i = 0; i < NUM; i++) {
    documents.save((i % 1000) + 1, {i:i}, function () {
      counter++;
      if (counter === NUM) {
        documents.compactDatabase();
      }
    
    });
  }
});


process.on('exit', function () {
  console.dir(process.memoryUsage());
});