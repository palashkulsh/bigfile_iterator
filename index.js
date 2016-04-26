var fs = require('fs');
var csv = require('csv');
var async = require('async');

/**
 *apiparam options filepath
 *apiparam iterator {function} function to be called with iterator(opts,cb)
 *****iterator accepts opts data {anything} you want to process
 *****iterator accepts opts outfilepath {string} file where iterator will save its output
 *****iterator accepts opts processedRowsCount {number} number of rows that have been processed till now
 *apiparam options outfilepath {string} path where result output will be written
 *apiparam options skipRowLevelErrors {bool} set to true if row level errors are to be skipped
 *apiparam finalcb {function} called when all the csv has been read
 */
function bigfileIteratorActual(options, iterator, finalcb) {
    //DebugFlow(getMyName(arguments))
    var totalRowCount = -1;
    var processedRows = 0;
    var bucket = [];
    var limit = 100;

    function txfn(row, index, callback) {
	////DebugFlow(getMyName(arguments))
	if (bucket.length < limit) {
	    bucket.push(row);
	    return setImmediate(function () {
		return callback(null, row);
	    });
	}
	var opts = {
	    data: bucket,
	    outfilepath: options.outfilepath,
	    processedRowsCount: processedRows
	};
	iterator(opts, function (err) {
	    processedRows += bucket.length;
	    if (!options.skipRowLevelErrors && err) {
		return callback(err);
	    }
	    bucket = [row];
	    return callback(null, row);
	});
    }

    if (!options.filepath) {
	return finalcb(new Error('options.filepath not provided'));
    }
    var csvObj = csv()
	.from(fs.createReadStream(options.filepath), {
	    delimiter: ',',
	    escape: '"',
	    columns: true
	})
	.transform(txfn, {
	    parallel: 1
	})
    // use single parallel
    //otherwise different threads will create race condition for processedRows and totalRowCount
	.on('end', function endFn(count) {
	    //DebugFlow(getMyName(arguments))
	    totalRowCount = count;
	    if (bucket.length) {
		var opts = {
		    data: bucket,
		    outfilepath: options.outfilepath,
		    processedRowsCount: processedRows
		};
		iterator(opts, function iteratorcb(err) {
		    //DebugFlow(getMyName(arguments))
		    processedRows += bucket.length;
		    if (!options.skipRowLevelErrors && err) {
			return finalcb(err);
		    }
		    bucket = [];
		    return finalcb();
		});

	    }
	})
	.on('error', function errfn(err) {
	    //DebugFlow(getMyName(arguments))
	    util.log(err);
	    return finalcb(err);
	});
}

var fileIterator={
    /**
     *apiparam options filepath it can be s3 path or local path
     *apiparam iterator {function} function to be called with iterator(opts,cb)
     *****iterator accepts opts data {anything} you want to process
     *****iterator accepts opts outfilepath {string} file where iterator will save its output
     *****iterator accepts opts processedRowsCount {number} number of rows that have been processed till now
     *apiparam options outfilepath {string} path where result output will be written
     *apiparam options skipRowLevelErrors {bool} set to true if row level errors are to be skipped
     *apiparam finalcb {function} called when all the csv has been read
     */
    bigfile_iterator:function bigfile_iterator(options, iterator, cb) {
	//check if options.filepath exists
	//if not then download the file from s3
	//now call bigfile_iterator on filename

	async.waterfall([
	    async.constant(options, iterator),
	    function check(options, iterator, callback) {
		fs.exists(options.filepath, function (exists) {
		    if (exists) {
			return callback(null, options, iterator);
		    } else {
			return callback(new Error('File not found'));
		    }
		});
	    },
	    function callIterator(options, iterator, callback) {
		bigfileIteratorActual(options, iterator, callback);
	    }
	], cb);
    }
}
module.exports=fileIterator;

(function(){
    if(require.main==module){
	var opts={
	    filepath:'/home/palashkulshreshtha/prod.csv',
	    outfilepath:'somepath'
	}
	    fileIterator.bigfile_iterator(opts,function (iteropts,callback){console.log(iteropts);return callback()},function (err){
		console.log('final done');
	    });
	}
})();
