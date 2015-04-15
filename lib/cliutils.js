
/* jshint esnext: true, node: true */
'use strict';

var rollbar = require('rollbar');
var log = require('loglevel');

rollbar.init(process.env.ROLLBAR_TOKEN, {environment: process.env.ROLLBAR_ENVIRONMENT});

exports.rejectHandler = function (err) {
  rollbar.handleError(err);
  log.error(err);
  log.error(err.message);
  log.error(err.stack);
  log.error("Aborting run.");
  process.exit(1);
};
