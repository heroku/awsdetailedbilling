/* jshint esnext: true, node: true */
'use strict';

/*******************************************************************************
Import finalized monthly DBRs.



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
  description: "Imports finalized (whole-month) detailed billing reports"
});

parser.addArgument(
  ['--force'], {
    action: 'storeConst',
    dest: 'force',
    help: 'Ignore and overwrite an existing staged DBR.',
    constant: true
  }
);

parser.addArgument(
  ['--no-vacuum'], {
    action: 'storeConst',
    dest: 'no_vacuum',
    help: 'Do not automatically run VACUUM following the import.',
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


// get last month
// verify that

dbr.getLatestFinalizedDBR()
  .then(importDBRCheck)
  .then(stageDBRCheck)
  .then(importDBR)
  .then(vacuum)
  .then(function() {
    cliUtils.runCompleteHandler(startTime, 0);
  })
  .catch(cliUtils.rejectHandler);


// Given a latest finalized DBR object, decide whether to import it
function importDBRCheck(finalizedDBR) {
  return redshift.hasMonth(finalizedDBR.Month).then(function(hasMonth) {
    if (hasMonth) {
      log.info(`No new DBRs to import.`);
      cliUtils.runCompleteHandler(startTime, 0);
    } else {
      return finalizedDBR;
    }
  });
}


// Given a DBR, (optionally) stage it
function stageDBRCheck(finalizedDBR) {
  return dbr.findStagedDBR(finalizedDBR.Month).then(
    function(stagedDBR) {
      let dbrMonth = stagedDBR.Month.format("MMMM YYYY");
      // DBR is staged!
      if (!args.force) {
        // No need to re-stage
        log.warn(`Using existing staged DBR for ${dbrMonth}.`);
        let s3uri = `s3://${args.staging_bucket}/${stagedDBR.Key}`;
        log.debug(`Staged s3uri: ${s3uri}`);
        return ({s3uri: s3uri, month: stagedDBR.Month});
      } else {
        // Force re-stage
        log.warn(`--force specified, overwriting staged DBR for ${dbrMonth}`);
        return dbr.stageDBR(stagedDBR.Month).then(function(s3uri) {
          return ({s3uri: s3uri, month: stagedDBR.Month});
        });
      }
    },
    function(err) {
      // DBR not staged. Stage then import.
      log.info(`Staging DBR for ${finalizedDBR.Month.format("MMMM YYYY")}.`);
      return dbr.stageDBR(finalizedDBR.Month).then(function(s3uri) {
        return ({s3uri: s3uri, month: finalizedDBR.Month});
      });
    }
  );
}


// Given an objet like {s3uri: <uri>, month: <moment>}
// Issue an import
function importDBR(params) {
  log.info(`Importing DBR for ${params.month.format("MMMM YYYY")}`);
  return redshift.importFullMonth(params.s3uri, params.month);
}


// Run VACUUM on the line_items table
function vacuum() {
  if (!args.no_vacuum) {
    log.info("Running VACUUM on line_items...");
    return redshift.vacuum(process.env.LINE_ITEMS_TABLE_NAME || 'line_items');
  } else {
    log.info("--no-vacuum specified, skiping vacuum.");
    return;
  }
}
