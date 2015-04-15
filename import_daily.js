/* jshint esnext: true, node: true */
'use strict';

/*******************************************************************************
Import month-to-date DBRs, overwriting the existing month-to-date.



*******************************************************************************/



var util = require('util');
var log = require('loglevel');
var ArgumentParser = require('argparse').ArgumentParser;
var rollbar = require('rollbar');
var moment = require('moment');

var BaseParser = require('./lib/baseparser.js');
var DBR = require('./lib/dbr.js');
var Redshift = require('./lib/redshift.js');
var cliUtils = require('./lib/cliutils.js');

rollbar.init(process.env.ROLLBAR_TOKEN, {environment: process.env.ROLLBAR_ENVIRONMENT});


var parser = new BaseParser({
  version: '0.0.1',
  addHelp: true,
  description: "Imports month-to-date detailed billing reports"
});

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
   .then(function(dbr) {
     log.info(`Importing ${dbr.Date.format("MMMM YYYY")} into month_to_date...`);
     return dbr.stageDBR(dbr.Date)
               .then(redshift.importMonthToDate);
   })
   .then(function() {
     log.info("Running VACUUM on month_to_date...");
     return redshift.vacuum('month_to_date');
   })
   .then(function() {
     let durationString = moment.utc(moment.utc() - startTime).format("HH:mm:ss.SSS");
     log.info("Run complete. Took ${durationString}");
   })
   .catch(cliUtils.rejectHandler);
