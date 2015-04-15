/* jshint esnext: true, node: true */
'use strict';

var util = require('util');
var fs = require('fs');
var path = require('path');

var log = require('loglevel');
var _ = require('lodash');
var moment = require('moment');
var AWS = require('aws-sdk');
var progress = require('progress-stream');
var prettyBytes = require('pretty-bytes');
var numeral = require('numeral');
var child_process = require('child_process');
var zlib = require('zlib');
var debounce = require('debounce');



module.exports = DBR;

function DBR(credentials, stagingCredentials, bucket, stagingBucket) {
  this.credentials = credentials;
  this.stagingCredentials = stagingCredentials;
  this.bucket = bucket;
  this.stagingBucket = stagingBucket;

  this.dbrClient = new AWS.S3(this.credentials);
  this.stagingClient = new AWS.S3(this.stagingCredentials);
}


// Download, unzip, gzip, upload a DBR to the staging bucket
// Returns a promise that resolves when everything is finished.
// Month is a UTC moment object for midnight on the first of the month.
DBR.prototype.stageDBR = function(month) {
  let self = this;
  return this.findDBR(month)
    .then(function(result) {
      return downloadDBR(result, self.dbrClient, self.bucket);
    })
    .then(function(result) {
      return processDBR(result, self.stagingClient, self.stagingBucket);
    });
};


// Find a DBR for a given month or raise an error
// Month is a UTC moment object for midnight on the first of the month.
DBR.prototype.findDBR = function(month) {
  let self = this;
  return new Promise(function (resolve, reject) {
    self.getDBRs()
        .then(function(dbrs) {
          let match = _.find(dbrs, function(d) { return month.isSame(d.Date); });
          if (match === undefined) {
            return reject(new Error(`Unable to find the DBR for ${month.format('MMMM YYYY')}.`));
          } else {
            return resolve(match);
          }
        });
  });
};

// Find a staged DBR for a given month or raise an error
// Month is a UTC moment object for midnight on the first of the month.
// TODO: is there a way to consolidate the handler in then() with findDBR()'s?
DBR.prototype.findStagedDBR = function(month) {
  let self = this;
  return new Promise(function (resolve, reject) {
    self.getStagedDBRs()
        .then(function(dbrs) {
          let match = _.find(dbrs, function(d) { return month.isSame(d.Date); });
          if (match === undefined) {
            return reject(new Error(`Unable to find the staged DBR for ${month.format('MMMM YYYY')}.`));
          } else {
            return resolve(match);
          }
        });
  });
};

// Get the contents of a bucket. Returns a promise which resolves with an array
// of bucket objects.
// Will not work with buckets containing > 1000 objects, but that's okay
// for our purposes here.
DBR.prototype.getBucketContents = function(client, bucket) {
  return new Promise(function (resolve, reject) {
    client.listObjects({Bucket: bucket}, function(err, data) {
      if (err) return reject(err);
      if ('Contents' in data) {
        return resolve(data.Contents);
      } else {
        return reject(`Bucket listObjects response didn't contain "Contents" key.`);
      }
    });
  });
};


// Get a listing of avalable DBRs
// Returns a promise which resolves with an date-sorted array of objects like:
// {Key: <filename>, Size: <bytes>, Date: <moment>}
DBR.prototype.getDBRs = function() {
  return this.getBucketContents(this.dbrClient, this.bucket)
             .then(processDBRBucketContents);
};


// Get a listing of staged DBRs
// Returns a promise which resolves with an date-sorted array of objects like:
// {Key: <filename>, Size: <bytes>, Date: <moment>}
DBR.prototype.getStagedDBRs = function() {
  return this.getBucketContents(this.stagingClient, this.stagingBucket)
             .then(processDBRBucketContents);
};


// Get the month-to-date DBR.
// Returns a promise which resolves with an object like:
// {Key: <filename>, Size: <bytes>, Date: <moment>}
DBR.prototype.getMonthToDateDBR = function() {
  return this.getDBRs()
             .then(function(dbrs) {
               if (dbrs.length === 0) {
                 throw new Error("There are no existing DBRs.");
               } else {
                 return dbrs[dbrs.length - 1];
               }
             });
};

DBR.prototype.composeStagedURI = function(dbrObject) {
  let gzFileName = path.basename(dbrObject.Key, '.zip') + '.gz';
  return `s3://${this.stagingBucket}/${gzFileName}`;
};




// =============================================================================
// Module-private stuff down here

var dbrPattern = /\d+-aws-billing-detailed-line-items-with-resources-and-tags-(\d{4})-(\d{2}).csv.[gz|zip]/;

function extractMonth(val) {
  let match = dbrPattern.exec(val);
  if (match === null) return null;
  let year = parseInt(match[1]);
  let month = parseInt(match[2]);
  return new moment.utc([year, month-1]);
}

// Take a bucket listing, filter out non-DBR entries, and return an array
// of objects ordered by the statement date (ascending). Each object has
// three properties: Key, Size, and Date:
//   Key:  the filename
//   Size: the size in bytes
//   Date: a utc moment object of the DBR month (midnight on first of the month)
function processDBRBucketContents(results) {
  let dbrs = [];
  // Filter only DBRs
  for (let result of results) {
    let month = extractMonth(result.Key);
    if (month === null) continue;
    // grab only the Key and Size properties
    let picked = _.pick(result, ['Key', 'Size']);
    // Add a Date property
    picked.Date = month;
    dbrs.push(picked);
  }
  return dbrs.sort(function (a, b) {
      if (a.Date < b.Date) return -1;
      else if (a.Date > b.Date) return 1;
      else return 0;
  });
}


// Downloads the specified DBR zip
// the argument is an object like:
//   {Key: <filename>, Size: <bytes>, Date: <moment>}
function downloadDBR(dbr, s3client, bucket) {
  const monthString = dbr.Date.format("MMM YYYY");
  log.info(`[${monthString}] (download): downloading from S3...`);
  return new Promise(function(resolve, reject) {
    let sourceParams = {
      Bucket: bucket,
      Key: dbr.Key
    };
    let outStream = fs.createWriteStream(dbr.Key);
    let downloadProgress = progress({
      length: dbr.Size,
      time: 1000
    });
    let request = s3client.getObject(sourceParams);

    downloadProgress.on("progress", function(dlprogress) {
      let percentage = numeral(dlprogress.percentage/100).format('00.0%');
      let eta = moment.duration(dlprogress.eta * 1000).humanize();
      log.info(`[${monthString}] (download): ${percentage} (${eta} at ${prettyBytes(dlprogress.speed)}/sec)`);
    });

    // Kick off the stream
    let zipfileStream = request.createReadStream();
    zipfileStream.pipe(downloadProgress)
                 .pipe(outStream);

    outStream.on('close', function() {
      log.info(`[${monthString}] (download): complete.`);
      return resolve(dbr);
    });
  });
}


// Processes the specified local DBR zip: unzip, gzip, upload to staging.
// the argument is an object like:
//   {Key: <filename>, Size: <bytes>, Date: <moment>}
function processDBR(dbr, s3client, bucket) {
  const monthString = dbr.Date.format("MMM YYYY");

  // Unzip, gzip, and upload to the staging bucket on S3
  log.info(`[${monthString}] (process): processing '${dbr.Key}'...`);

  // In theory, zipfiles can contain multiple files
  // We know that the DBR zip has only one file inside, the DBR CSV
  return new Promise(function(resolve, reject) {
    var uncompressedLength = parseInt(child_process.execSync(
      `zipinfo -t ${dbr.Key} | cut -d ' ' -f 3`, {encoding: 'utf8'}
    ));

    // Hack off the '.zip'
    var plainFileName = path.basename(dbr.Key, '.zip');

    // For monitoring unzip progress
    var unzipProgress = progress({time: 10000, length: uncompressedLength}, function(uzprogress) {
      let percentage = numeral(uzprogress.percentage/100).format('00.0%');
      let eta = moment.duration(uzprogress.eta * 1000).humanize();
      log.info(`[${monthString}] (process-unzip): ${percentage} (${eta} at ${prettyBytes(uzprogress.speed)}/sec)`);
    });

    // For monitoring gzip progress.
    // From this point forward in the stream, we don't know the stream length as
    // we don't know how much the stream will compress down to until it's done.
    var gzipProgress = progress({time: 10000}, function(gzprogress) {
      log.info(`[${monthString}] (process-gzip): ${prettyBytes(gzprogress.transferred)} at ${prettyBytes(gzprogress.speed)}/sec`);
    });

    // Hook up every part of the stream prior to the HTTP upload to S3
    // Stream not flowing at this point! Triggered by request.send() below.
    var unzipGzipStream = child_process.spawn('unzip', ['-p', `./${dbr.Key}`])
                                       .stdout
                                       .pipe(unzipProgress)
                                       .pipe(zlib.createGzip())
                                       .pipe(gzipProgress);

    // Prepare the upload to S3 with the stream as the body
    var requestParams = {
      Bucket: bucket,
      Key: `${plainFileName}.gz`,
      Body: unzipGzipStream
    };
    var request = s3client.upload(requestParams);
    request.on('httpUploadProgress', debounce(function(progress) {
      log.info(`[${monthString}] (process-upload): ${prettyBytes(progress.loaded)}`);
    }, 1000, true));

    // Fire the upload request, gets the stream flowing.
    request.send(function(err, data) {
      if (err) return reject(err);
      log.info(`[${monthString}] (process-upload): complete.`);
      return resolve(`s3://${requestParams.Bucket}/${requestParams.Key}`);
    });
  });
}
