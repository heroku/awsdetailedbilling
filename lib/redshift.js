/* jshint esnext: true, node:true */
'use strict';

var util = require('util');
var log = require('loglevel');
var moment = require('moment');
var pg = require('pg');
var types = require('pg').types

// Redshift doesn't have TIMESTAMP WITH TIME ZONE
// All dates will therefore come back here as localtime
// This forces dates to come back as UTC.
types.setTypeParser(1082, function(stringVal) {
  return new Date(stringVal);
});

module.exports = Redshift;

function Redshift(connString) {
  this.connString = connString;
}

Redshift.prototype.executeQuery = function(query, transform) {
  var self = this;
  return new Promise(function(resolve, reject) {
    pg.connect(self.connString, function(err, client, done) {
      if (err) throw err;
      client.query(query, function(err, result) {
        if (err) throw err;
        if (transform !== undefined) {
          result = transform(result);
        }
        done(client);
        return resolve(result);
      });
    });
  });
};

Redshift.prototype.getScalar = function(query, keyName) {
  let transform = function(result) {
    let rowzero = result.rows[0];
    if (keyName !== undefined) {
      return rowzero[keyName];
    } else {
      // return the value of the first key
      return rowzero[Object.keys(rowzero)[0]];
    }
  };
  return this.executeQuery(query, transform);
};

Redshift.prototype.getRowCount = function(query) {
  let transform = function(result) {
    return result.rowCount;
  };
  return this.executeQuery(query, transform);
};

Redshift.prototype.checkTableExists = function(tableName) {
  let query = `
    SELECT EXISTS (
      SELECT 1
      FROM   pg_catalog.pg_class c
      JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE  n.nspname = 'public'
      AND    c.relname = '${tableName}'
      AND    c.relkind = 'r'    -- only tables
    ) as exists;`;

  let transform = function(result) {
    return result.rows[0].exists === 't';
  };
  return this.executeQuery(query, transform);
};

Redshift.prototype.latestFullMonth = function() {
  let query = 'SELECT MAX(statement_month) FROM line_items';
  return this.getScalar(query, 'max').then(function(date) {
    return moment(date);
  });
};

Redshift.prototype.emptyMonthToDate = function() {
  let query = `DELETE FROM month_to_date`;
  return this.getRowCount(query);
};
