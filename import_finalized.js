'use strict'

/*******************************************************************************
Import finalized monthly DBRs.
*******************************************************************************/

var util = require('util')

var log = require('loglevel')
var rollbar = require('rollbar')
var moment = require('moment')

var BaseParser = require('./lib/baseparser.js')
var DBR = require('./lib/dbr.js')
var Redshift = require('./lib/redshift.js')
var cliUtils = require('./lib/cliutils.js')

rollbar.init(process.env.ROLLBAR_TOKEN, {environment: process.env.ROLLBAR_ENVIRONMENT})
rollbar.handleUncaughtExceptions(process.env.ROLLBAR_TOKEN,
                                 {exitOnUncaughtException: true})

var parser = new BaseParser({
  version: '0.0.1',
  addHelp: true,
  description: 'Imports finalized (whole-month) detailed billing reports'
})

parser.addArgument(
  ['--force'], {
    action: 'storeConst',
    dest: 'force',
    help: 'Ignore and overwrite an existing staged DBR.',
    constant: true
  }
)

parser.addArgument(
  ['--specific'], {
    help: "Import a specific month's DBR. Specified in YYYY-MM format."
  }
)

parser.addArgument(
  ['--prune-months'], {
    help: 'The amount of history (in number of months) to retain in Redshift',
    type: 'int'
  }
)

var args = parser.parseArgs()

if (args.debug) {
  log.setLevel('debug')
  log.debug('Debugging output enabled.')
} else {
  log.setLevel('info')
}
log.debug(`Resolved invocation arguments were:\n${util.inspect(args)}`)

if (args.specific !== null && args.prune_months !== null) {
  log.error('The "--specific" and "--prune-months" options are mutually exclusive.')
  log.error('--prune-months can only be invoked when importing the latest DBR.')
  log.error('Aborting.')
  process.exit(1)
}

// Instantiate a DBR object to work with.
var dbrClientOptions = {
  accessKeyId: args.source_key,
  secretAccessKey: args.source_secret
}

var stagingClientOptions = {
  accessKeyId: args.staging_key,
  secretAccessKey: args.staging_secret
}

var dbr = new DBR(dbrClientOptions, stagingClientOptions,
                  args.source_bucket, args.staging_bucket)

// Instantiate a Redshift object to work with.
var redshift = new Redshift(args.redshift_uri, {
      key: args.staging_key,
      secret: args.staging_secret
})

let startTime = moment.utc()

chooseDBR()
  .then(importDBRCheck)
  .then(stageDBRCheck)
  .then(importDBR)
  .then(vacuum)
  .then(function () {
    cliUtils.runCompleteHandler(startTime, 0)
  })
  .catch(cliUtils.rejectHandler)

function chooseDBR () {
  return new Promise(function (resolve, reject) {
    if (args.specific) {
      log.debug(`Invoked with --specific ${args.specific}.`)
      try {
        let match = /^(\d{4})-(\d{2})$/.exec(args.specific)
        if (match === null) {
          return reject('--specific requires a year and month parameter in the form of YYYY-MM')
        }
        // moment.utc month argument is zero-indexed
        let month = moment.utc([match[1], match[2] - 1])
        log.debug(`Attempting to import ${month.toISOString()}`)
        return resolve(dbr.findDBR(month))
      } catch (err) {
        return reject(err)
      }
    } else {
      log.debug(`Invoked without --specific. Targeting latest finalized DBR...`)
      return resolve(dbr.getLatestFinalizedDBR())
    }
  })
}

// Given a latest finalized DBR object, decide whether to import it
function importDBRCheck (finalizedDBR) {
  return redshift.hasMonth(finalizedDBR.Month).then(function (hasMonth) {
    if (hasMonth) {
      log.info(`No new DBRs to import.`)
      cliUtils.runCompleteHandler(startTime, 0)
    } else {
      return finalizedDBR
    }
  })
}

// Given a DBR, (optionally) stage it
function stageDBRCheck (finalizedDBR) {
  return dbr.findStagedDBR(finalizedDBR.Month).then(
    function (stagedDBR) {
      let dbrMonth = stagedDBR.Month.format('MMMM YYYY')
      // DBR is staged!
      if (!args.force) {
        // No need to re-stage
        log.warn(`Using existing staged DBR for ${dbrMonth}.`)
        let s3uri = `s3://${args.staging_bucket}/${stagedDBR.Key}`
        log.debug(`Staged s3uri: ${s3uri}`)
        return ({s3uri: s3uri, month: stagedDBR.Month})
      } else {
        // Force re-stage
        log.warn(`--force specified, overwriting staged DBR for ${dbrMonth}`)
        return dbr.stageDBR(stagedDBR.Month).then(function (s3uri) {
          return ({s3uri: s3uri, month: stagedDBR.Month})
        })
      }
    },
    function (err) {
      // DBR not staged. Stage then import.
      log.debug(`DBR not staged: ${err}`)
      log.info(`Staging DBR for ${finalizedDBR.Month.format('MMMM YYYY')}.`)
      return dbr.stageDBR(finalizedDBR.Month).then(function (s3uri) {
        return ({s3uri: s3uri, month: finalizedDBR.Month})
      })
    }
  )
}

// Given an object like {s3uri: <uri>, month: <moment>}
// Execute the import.
function importDBR (params) {
  log.info(`Importing DBR for ${params.month.format('MMMM YYYY')}`)
  if (args.prune_months !== null) {
    let pruneThreshold = moment(params.month)
      .subtract(args.prune_months, 'months')
      .format('MMMM YYYY')
    log.info(`... and pruning months prior to ${pruneThreshold}`)
    return redshift.importFullMonth(params.s3uri, params.month, args.prune_months)
  } else {
    return redshift.importFullMonth(params.s3uri, params.month)
  }
}

// Run VACUUM on the line_items table
function vacuum () {
  if (!args.no_vacuum) {
    log.info('Running VACUUM on line_items...')
    return redshift.vacuum(process.env.LINE_ITEMS_TABLE_NAME || 'line_items')
  } else {
    log.info('--no-vacuum specified, skiping vacuum.')
    return
  }
}
