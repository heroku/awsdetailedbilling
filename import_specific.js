/* jshint esnext: true, node: true */
'use strict';

/*******************************************************************************
Import a specific finalized DBR month.



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
  description: "Imports a specific detailed billing report to the line_items table."
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
    help: 'Do not automatically run VACUUM following the import. Useful when importing multiple DBRs in parallel.',
    constant: true
  }
);


parser.addArgument(
  ['year'], {
    action: 'store',
    help: 'The year of the target DBR to import.',
    type: 'int',
    metavar: 'year'
  }
);

parser.addArgument(
  ['month'], {
    action: 'store',
    help: 'The month of the target DBR to import.',
    type: 'int',
    metavar: 'month'
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

process.exit(-123);

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

let target = moment.utc([args.year, args.month, 1]);
let startTime = moment.utc();

dbr.findStagedDBR(target)
  .catch(cliUtils.rejectHandler)       // fail early if something broke
  .then(function(stagedDBR) {
    // We do have that staged already!
    let dbrMonth = stagedDBR.Date.format('MMMM YYYY');
    log.debug(`Found staged ${dbrMonth}!`);
    if (!args.force) {
      log.warn(`Using existing staged DBR for ${dbrMonth}.`);
      let s3uri = `s3://${args.staging_bucket}/${stagedDBR.key}`;
      return redshift.importFullMonth(s3uri, stagedDBR.Date);
    } else {
      log.warn(`--force specified, overwriting staged DBR for ${dbrMonth}`);
      return dbr.stageDBR(stagedDBR.date).then(function(s3uri) {
        return redshift.importFullMonth(s3uri, stagedDBR.Date);
      });
    }
  }, function(reason) {
    // We don't have that DBR staged!
    return dbr.stageDBR(target).then(function(s3uri) {
      return redshift.importFullMonth(s3uri, target);
    });
  })
  .then(function(result) {
    // import was successful
    if (!args.no_vacuum) {
      log.info("Running VACUUM...");
      return redshift.vacuum();
    } else {
      log.info("--no-vacuum specified, skiping vacuum.");
      return;
    }
  })
  .then(function() {
    let durationString = moment.utc(moment.utc() - startTime).format("HH:mm:ss.SSS");
    log.info(`Run complete. Took ${durationString}`);
  })
  .catch(cliUtils.rejectHandler);
