'use strict'

var rollbar = require('rollbar')
var log = require('loglevel')
var moment = require('moment')

rollbar.init(process.env.ROLLBAR_TOKEN, {environment: process.env.ROLLBAR_ENVIRONMENT})

exports.rejectHandler = function (err) {
  rollbar.handleError(err)
  log.error(err)
  log.error(err.message)
  log.error(err.stack)
  log.error('Aborting run.')
  process.exit(1)
}

exports.runCompleteHandler = function (startTime, exitCode) {
  let durationString = moment.utc(moment.utc() - startTime).format('HH:mm:ss.SSS')
  log.info(`Run complete. Took ${durationString}`)
  process.exit(exitCode || 0)
}
