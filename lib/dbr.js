/* jshint esnext: true, node: true */
'use strict';

var util = require('util');
var fs = require('fs');

var log = require('loglevel');
var _ = require('lodash');
var moment = require('moment');
var AWS = require('aws-sdk');


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
DBR.prototype.stageDBR = function(month) {
};


// Find a DBR for a given month or raise an error
// Argument is a UTC moment object representing midnight on the first of the
// desired month.
DBR.prototype.findDBR = function(month) {
  let self = this;
  return new Promise(function (resolve, reject) {
    self.getDBRs()
        .then(function(dbrs) {
          let match = _.find(dbrs, function(d) { return month.isSame(d.Date); });
          if (match === undefined) {
            throw new Error(`Unable to find the DBR for ${month.format('MMMM YYYY')}.`);
          } else {
            resolve(match);
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
      if (err) throw err;
      if ('Contents' in data) {
        resolve(data.Contents);
      } else {
        reject(`Bucket listObjects response didn't contain "Contents" key.`);
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
