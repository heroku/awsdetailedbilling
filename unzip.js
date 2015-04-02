/* jshint esnext: true */

var util = require('util');
var fs = require('fs');
var AWS = require('aws-sdk');
var progress = require('progress-stream');
var prettyBytes = require('pretty-bytes');
var moment = require('moment');
var numeral = require('numeral');
var log = require('loglevel');
var pg = require('pg');
var child_process = require('child_process');
var zlib = require('zlib');
var debounce = require('debounce');
var ArgumentParser = require('argparse').ArgumentParser;

var rollbar = require('rollbar');
rollbar.init(process.env.ROLLBAR_TOKEN);

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

// We'll need these handy in various places
var monthMatch = /(\d{4})-(\d{2})/.exec(args.file);
var year = monthMatch[1];
var month = monthMatch[2];
var monthString = `${year}_${month}`;
var monthDateString = `${year}-${month}-01`;

// S3 Clients for the DBR and staging buckets
var dbrClient = new AWS.S3(dbrClientOptions);
var stagingClient = new AWS.S3(stagingClientOptions);


// ==============================================================
// Each of the major steps is a function which returns a promise.
// ==============================================================

var downloadFile = function(bucket, key) {
  // Downloads the specified DBR zip
  log.info(`${monthString} (download): downloading '${key}' from S3...`);
  return new Promise(function(resolve, reject) {
    var sourceParams = {
      Bucket: bucket,
      Key: key
    };
    var outStream = fs.createWriteStream(key);

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
      var durationString = moment.utc(moment.utc() - startTime).format("HH:mm:ss.SSS");
      log.info(`${monthString} (download): complete (${durationString})`);
      resolve(key);
    });
  });
};

var processZipFile = function(zipFileName) {
  // Unzip, gzip, and upload to the staging bucket on S3
  log.info(`${monthString} (process): processing '${zipFileName}'...`);

  // In theory, zipfiles can contain multiple files
  // We know that the DBR zip has only one file inside, the DBR CSV
  return new Promise(function(resolve, reject) {
    var uncompressedLength = parseInt(child_process.execSync(
      `zipinfo -t ${zipFileName} | cut -d ' ' -f 3`, {encoding: 'utf8'}
    ));

    // Hack off the '.zip'
    var plainFileName = zipFileName.substr(0, zipFileName.length - 4);

    // For monitoring unzip progress
    var unzipProgress = progress({time: 10000, length: uncompressedLength}, function(progress) {
      percentage = numeral(progress.percentage/100).format('00.0%');
      eta = moment.duration(progress.eta * 1000).humanize();
      log.info(`${monthString} (process-unzip): ${percentage} (${eta} at ${prettyBytes(progress.speed)}/sec)`);
    });

    // For monitoring gzip progress.
    // From this point forward in the stream, we don't know the stream length as
    // we don't know how much the stream will compress down to until it's done.
    var gzipProgress = progress({time: 10000}, function(progress) {
      log.info(`${monthString} (process-gzip): ${prettyBytes(progress.transferred)} at ${prettyBytes(progress.speed)}/sec`);
    });

    // Hook up every part of the stream prior to the HTTP upload to S3
    // Stream not flowing at this point! Triggered by request.send() below.
    var unzipGzipStream = child_process.spawn('unzip', ['-p', './' + zipFileName])
                                       .stdout
                                       .pipe(unzipProgress)
                                       .pipe(zlib.createGzip())
                                       .pipe(gzipProgress);

    // Prepare the upload to S3 with the stream as the body
    var requestParams = {
      Bucket: process.env.STAGING_BUCKET,
      Key: `${plainFileName}.gz`,
      Body: unzipGzipStream
    };
    var request = stagingClient.upload(requestParams);
    request.on('httpUploadProgress', debounce(function(progress) {
      log.info(`${monthString} (process-upload): ${prettyBytes(progress.loaded)}`);
    }, 1000, true));

    // Fire the upload request, gets the stream flowing.
    request.send(function(err, data) {
      if (err) throw err;
      var durationString = moment.utc(moment.utc() - startTime).format("HH:mm:ss.SSS");
      log.info(`${monthString} (upload): complete (${durationString})`);
      resolve(`s3://${requestParams.Bucket}/${requestParams.Key}`);
    });

  });
};

var importToRedshift = function(s3uri) {
  // Import the gzipped DBR from the staging bucket into a staging table on
  // redshift. Add the statement_month column and then copy from staging into
  // line_items, then drop the staging table.
  log.info(`${monthString} (import): importing to redshift...`);
  return new Promise(function(resolve, reject) {
    var client = new pg.Client(args.redshift_url);
    client.connect(function(err) {
      if (err) throw err;
      var stagingTableName = `staging_${monthString}`;
      var query = `
        BEGIN;
          -- can't create and alter because statement_month is the sortkey
          -- must create from scratch
          -- CREATE TABLE ${stagingTableName} (LIKE line_items);
          -- ALTER TABLE ${stagingTableName} DROP COLUMN statement_month;

          CREATE TABLE IF NOT EXISTS ${stagingTableName} (
            invoice_id TEXT,
            payer_account_id TEXT,
            linked_account_id TEXT,
            record_type TEXT,
            record_id TEXT,
            product_name TEXT,
            rate_id TEXT,
            subscription_id TEXT,
            pricing_plan_id TEXT,
            usage_type TEXT,
            operation TEXT,
            availability_zone TEXT,
            reserved_instance TEXT,
            item_description TEXT,
            usage_start_date TIMESTAMP,
            usage_end_date TIMESTAMP,
            usage_quantity FLOAT8,
            blended_rate NUMERIC(18,11),
            blended_cost NUMERIC(18,11),
            unblended_rate NUMERIC(18,11),
            unblended_cost NUMERIC(18,11),
            resource_id TEXT,
            cloud TEXT,
            slot TEXT,
            PRIMARY KEY(record_id)
          ) DISTSTYLE EVEN;

          COPY ${stagingTableName}
            FROM '${s3uri}'
            CREDENTIALS 'aws_access_key_id=${stagingClientOptions.accessKeyId};aws_secret_access_key=${stagingClientOptions.secretAccessKey}'
            GZIP CSV IGNOREHEADER 1;
          ALTER TABLE ${stagingTableName} ADD COLUMN statement_month DATE DEFAULT '${monthDateString}';
          INSERT INTO line_items SELECT * FROM ${stagingTableName};
          DROP TABLE ${stagingTableName};
        COMMIT;
        -- ANALYZE line_items;
        -- VACUUM line_items;
      `;
      log.debug(query);
      client.query(query, function(err, result) {
        if (err) throw err;
        var durationString = moment.utc(moment.utc() - startTime).format("HH:mm:ss.SSS");
        log.info(`${monthString} (import): complete (${durationString})`);
        resolve(s3uri);
      });
    });
  });
};


// Kick off the promise chain.
var startTime = moment.utc();
downloadFile(args.source_bucket, args.file)
  .then(processZipFile)
  .then(importToRedshift)
  .then(function(s3uri) {
    var durationString = moment.utc(moment.utc() - startTime).format("HH:mm:ss.SSS");
    log.info(`${monthString}: Import complete! Took ${durationString}`);
    process.exit();
  })
  .catch(function(err) {
    var durationString = moment.utc(moment.utc() - startTime).format("HH:mm:ss.SSS");
    log.error(`${monthString}: Something went terribly wrong after ${durationString}`);
    log.error(err);
    log.error(err.stack);
		rollbar.handleError(err);
    process.exit();
  });
