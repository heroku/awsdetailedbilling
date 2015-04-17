/* jshint esnext: true, node: true */
'use strict';

/*******************************************************************************
Import month-to-date DBRs, overwriting the existing month-to-date.



*******************************************************************************/



var util = require('util');
var path = require('path');

var log = require('loglevel');
var ArgumentParser = require('argparse').ArgumentParser;
var rollbar = require('rollbar');
var moment = require('moment');

var BaseParser = require('./lib/baseparser.js');
var DBR = require('./lib/dbr.js');
var Redshift = require('./lib/redshift.js');
var cliUtils = require('./lib/cliutils.js');

rollbar.init(process.env.ROLLBAR_TOKEN, {environment: process.env.ROLLBAR_ENVIRONMENT});
rollbar.handleUncaughtExceptions(process.env.ROLLBAR_TOKEN,
                                 {exitOnUncaughtException: true});


var parser = new BaseParser({
  version: '0.0.1',
  addHelp: true,
  description: "Imports month-to-date detailed billing reports"
});

parser.addArgument(
  ['--no-stage'], {
    action: 'storeConst',
    dest: 'no_stage',
    help: 'Use an existing staged month-to-date DBR.',
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
log.debug(`Resolved invocation arguments were:\n${util.inspect(args)}`);

// Instantiate a DBR object to work with.
var dbrClientOptions = {
  accessKeyId: args.source_key,
  secretAccessKey: args.source_secret
};

var stagingClientOptions = {
  accessKeyId: args.staging_key,
  secretAccessKey: args.staging_secret
};

var dbr = new DBR(dbrClientOptions, stagingClientOptions,
                  args.source_bucket, args.staging_bucket);


// Instantiate a Redshift object to work with.
var redshift = new Redshift(args.redshift_uri, {
      key: args.staging_key,
      secret: args.staging_secret
});

let startTime = moment.utc();

dbr.getMonthToDateDBR()
  .then(stageDBRCheck)
  .then(importDBR)
  .then(vacuum)
  .then(function() {
    cliUtils.runCompleteHandler(startTime, 0);
  })
  .catch(cliUtils.rejectHandler);


// Determine whether to stage the latest month-to-date DBR or reuse existing
function stageDBRCheck(monthToDateDBR) {
  log.info(`Found month-to-date for ${monthToDateDBR.Month.format("MMMM YYYY")}...`);
  if (args.no_stage) {
    let s3uri = dbr.composeStagedURI(monthToDateDBR);
    log.info(`--no-stage specified, Attempting to use existing staged month-to-date DBR`);
    log.debug(`Importing from ${s3uri}`);
    return s3uri;
  } else {
    log.info(`Staging DBR file for ${monthToDateDBR.Month.format("MMMM YYYY")}.`);
    return dbr.stageDBR(monthToDateDBR.Month);
  }
}


// Import the staged month-to-date DBR
// TODO if we just chain like .then(redshift.importMonthToDate), it fails
// because "this" inside importMonthToDate will be undefined. Why?
function importDBR(s3uri) {
  log.info(`Importing ${monthToDateDBR.Month.format("MMMM YYYY")} into month_to_date...`);
  return redshift.importMonthToDate(s3uri);
}


// Run VACUUM on the month_to_date table
function vacuum() {
  if (!args.no_vacuum) {
    log.info("Running VACUUM on line_items...");
    return redshift.vacuum('month_to_date');
  } else {
    log.info("--no-vacuum specified, skiping vacuum.");
    return;
  }
}
