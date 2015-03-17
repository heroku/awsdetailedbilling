/* jshint esnext: true */

var util = require('util');
var fs = require('fs');
var AWS = require('aws-sdk');
var yauzl = require('yauzl');
var progress = require('progress-stream');
var prettyBytes = require('pretty-bytes');
var moment = require('moment');
var numeral = require('numeral');
var log = require('loglevel');
var pg = require('pg');

var ArgumentParser = require('argparse').ArgumentParser;

var parser = new ArgumentParser({
	version: '0.0.1',
	addHelp: true,
	description: "Unzips detailed billing reports"
});

parser.addArgument(
	['-i', '--source-bucket'], {
		help: 'The source S3 bucket name',
		defaultValue: process.env.DBR_BUCKET
	}
);

parser.addArgument(
	['-o', '--dest-bucket'], {
		help: 'The destination S3 bucket name',
		defaultValue: process.env.STAGING_BUCKET
	}
);

parser.addArgument(
	['-r', '--redshift-url'], {
		help: 'The destination S3 bucket name',
		defaultValue: process.env.REDSHIFT_URL
	}
);

parser.addArgument(
	['-t', '--target-table'], {
		help: 'The redshift table to copy data into',
		defaultValue: 'line_items'
	}
);

parser.addArgument(
	['-f', '--file'], {
		help: 'The file to unzip and copy',
		required: true
	}
);

parser.addArgument(
	['-d', '--debug'], {
		action: 'storeConst',
		dest: 'debug',
		help: 'Turn on debugging output',
		constant: true
	}
);

var args = parser.parseArgs();

if (args.debug) {
	log.setLevel('debug');
	log.debug("Debugging output enabled.");
} else {
	log.setLevel('info');
}
log.debug(args);

var dbrClientOptions = {
	accessKeyId: process.env.DBR_AWS_ACCESS_KEY_ID,
	secretAccessKey: process.env.DBR_AWS_SECRET_ACCESS_KEY
};

var stagingClientOptions = {
	accessKeyId: process.env.IDAN_AWS_ACCESS_KEY_ID,
	secretAccessKey: process.env.IDAN_AWS_SECRET_ACCESS_KEY
};

var sourceParams = {
	Bucket: args.source_bucket,
	Key: args.file
};

var monthString = /\d{4}-\d{2}/.exec(args.file)[0];

var dbrClient = new AWS.S3(dbrClientOptions);
var stagingClient = new AWS.S3(stagingClientOptions);

var downloadFile = new Promise(function(resolve, reject) {
	var outStream = fs.createWriteStream(sourceParams.Key);

	var downloadProgress = progress({
		length: 0,
		time: 1000
	});
	downloadProgress.on("progress", function(progress) {
		percentage = numeral(progress.percentage/100).format('00.0%');
		eta = moment.duration(progress.eta * 1000).humanize();
		log.info(`${monthString} (download): ${percentage} (${eta} at ${prettyBytes(progress.speed)}/sec)`);
	});

	var request = dbrClient.getObject(sourceParams);
	request.on('httpHeaders', function(status, headers, resp) {
		totalLength = parseInt(headers['content-length'], 10);
		downloadProgress.setLength(totalLength);
	});

	var zipfileStream = request.createReadStream();
	zipfileStream.pipe(downloadProgress)
				       .pipe(outStream);

	outStream.on('close', function() {
		resolve(args.file);
	});
});

var unzipFile = function(zipFileName) {
	// In theory, this handles multiple files inside a zip
	// We know that this zipfile has only one file inside, the CSV
	// hence we are resolving on the first successful unzipped file
	return new Promise(function(resolve, reject) {
		yauzl.open(zipFileName, function(err, file) {
			if (err) throw err;
			file.on('entry', function(entry) {
				if (err) throw err;
				outStream = fs.createWriteStream(entry.fileName);
				file.openReadStream(entry, function(err, readStream){
					if (err) throw err;
					readStream.pipe(outStream);
					outStream.on('close', function() {
						resolve(entry.fileName);
					});
				});
			});
		});
	});
};

var uploadFile = function(unzippedFileName) {
	return new Promise(function(resolve, reject) {

		var unzippedLength = fs.statSync(unzippedFileName).size;
		var unzippedReadStream = fs.createReadStream(unzippedFileName);

		var uploadProgress = progress({
			length: unzippedLength,
			time: 1000
		});
		uploadProgress.on("progress", function(progress) {
			percentage = numeral(progress.percentage/100).format('00.0%');
			eta = moment.duration(progress.eta * 1000).humanize();
			log.info(`${monthString} (upload): ${percentage} (${eta} at ${prettyBytes(progress.speed)}/sec)`);
		});

		var destParams = {
			Bucket: args.dest_bucket,
			Key: unzippedFileName,
			Body: unzippedReadStream.pipe(uploadProgress)
		};

		stagingClient.upload(destParams, function(err, data) {
			if (err) throw err;
			resolve(`s3://${destParams.Bucket}/${destParams.Key}`);
		});
	});
};

var importToRedshift = function(s3uri) {
	return new Promise(function(resolve, reject) {
		var client = new pg.Client(args.redshift_url);
		client.connect(function(err) {
			if (err) throw err;
			var query = `COPY ${args.target_table} FROM '${s3uri}' credentials 'aws_access_key_id=${stagingClientOptions.accessKeyId};aws_secret_access_key=${stagingClientOptions.secretAccessKey}' csv ignoreheader 1;`;
			log.debug(query);
			client.query(query, function(err, result) {
				if (err) {
					reject(`Error in COPY FROM: ${err}`);
					return;
				}
				resolve(s3uri);
			});
		});
	});
};

downloadFile.then(function(result) {
	log.info(`${monthString} (unzip): Unzipping ${result}...`);
	return unzipFile(result);
}).then(function(result) {
	log.info(`${monthString} (unzip): Unzipped ${result}`);
	log.info(`${monthString} (upload): Uploading to ${args.dest_bucket}...`);
	return uploadFile(result);
}).then(function(result) {
	log.info(`${monthString} (upload): Uploaded ${result}`);
	log.info(`${monthString} (import): Importing to Redshift...`);
	return importToRedshift(result);
}).then(function(result) {
	log.info(`${monthString} (import): COPY FROM issued successfully`);
	process.exit();
}).catch(function(err) {
	log.error(err);
});
