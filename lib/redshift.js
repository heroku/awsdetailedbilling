/* jshint esnext: true, node:true */
'use strict';

var util = require('util');
var log = require('loglevel');
var moment = require('moment');
var pg = require('pg');
var types = require('pg').types;

// Redshift doesn't have TIMESTAMP WITH TIME ZONE
// All dates will therefore come back here as localtime
// This forces dates to come back as UTC.
types.setTypeParser(1082, function(stringVal) {
  return new Date(stringVal);
});


module.exports = Redshift;

function Redshift(connString, s3credentials) {
  this.connString = connString;
  this.s3credentials = s3credentials;
}


// Execute a query, using the query pool
// Return a promise which resolves with the output of the query.
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


// Execute a query where the desired ouput is a single scalar value.
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


// Execute a query where the desired output is a count of affected rows
// (for example, DELETE FROM queries).
Redshift.prototype.getRowCount = function(query) {
  let transform = function(result) {
    return result.rowCount;
  };
  return this.executeQuery(query, transform);
};


// Check that a table exists. Won't match on views or other table-like things.
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


// Determine the month of the latest finalized DBR to be imported into Redshift.
Redshift.prototype.latestFullMonth = function() {
  let query = 'SELECT MAX(statement_month) FROM line_items';
  return this.getScalar(query, 'max').then(function(date) {
    return moment(date);
  });
};


// Import a finalized (full-month) DBR into Redshift.
// First, create a staging table and COPY FROM into that.
// Then, add the statement_month column, and copy it all to line_items.
// Then drop the staging table.
Redshift.prototype.importFullMonth = function(s3uri, month) {
  const monthString = month.format("YYYY_MM");
  const monthDateString = month.format("YYYY-MM-01");
  const stagingTableName = `staging_${monthString}`;

  // Normally, creating the staging table would look like:
  //   CREATE TABLE ${stagingTableName} (LIKE line_items);
  // However you can't alter the staging table to drop statement_month because
  // it is specified as the SORTKEY, and that can't be touched in existing
  // tables. So, we create the staging table from scratch.
  let query = `
    BEGIN;
      CREATE TABLE IF NOT EXISTS ${stagingTableName} (
        invoice_id TEXT,
        payer_account_id TEXT,
        linked_account_id TEXT,
        record_type TEXT,
        record_id TEXT,
        product_name TEXT,
        rate_id TEXT,
        subscription_id TEXT,
        pricing_plan_id TEXT,
        usage_type TEXT,
        operation TEXT,
        availability_zone TEXT,
        reserved_instance TEXT,
        item_description TEXT,
        usage_start_date TIMESTAMP,
        usage_end_date TIMESTAMP,
        usage_quantity FLOAT8,
        blended_rate NUMERIC(18,11),
        blended_cost NUMERIC(18,11),
        unblended_rate NUMERIC(18,11),
        unblended_cost NUMERIC(18,11),
        resource_id TEXT,
        cloud TEXT,
        slot TEXT,
        PRIMARY KEY(record_id)
      ) DISTSTYLE EVEN;

      COPY ${stagingTableName}
        FROM '${s3uri}'
        CREDENTIALS 'aws_access_key_id=${this.s3credentials.key};aws_secret_access_key=${this.s3credentials.secret}'
        GZIP CSV IGNOREHEADER 1;
      ALTER TABLE ${stagingTableName} ADD COLUMN statement_month DATE DEFAULT '${monthDateString}';
      DELETE FROM line_items WHERE statement_month = '${monthDateString}';
      INSERT INTO line_items SELECT * FROM ${stagingTableName};
      DROP TABLE ${stagingTableName};
    COMMIT;
  `;

  return this.executeQuery(query);
};


// Import the month-to-date DBR into the month_to_date table, clobbering
// whatever was already there.
Redshift.prototype.importMonthToDate = function(s3uri) {
  // In theory we could use TRUNCATE here instead of DELETE FROM month_to_date
  // It would obviate the need for VACUUMing afterwards, but it doesn't work
  // inside a transaction (it commits the transaction immediately).
  // TODO decide whether that's a good tradeoff.
  let query = `
    BEGIN;
      DELETE FROM month_to_date;
      COPY month_to_date
        FROM '${s3uri}'
        CREDENTIALS 'aws_access_key_id=${this.s3credentials.key};aws_secret_access_key=${this.s3credentials.secret}'
        GZIP CSV IGNOREHEADER 1;
    COMMIT;
  `;
  return this.executeQuery(query);
};
