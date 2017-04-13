'use strict'

var log = require('loglevel')
var moment = require('moment')
var pg = require('pg')
var types = require('pg').types

// Redshift doesn't have TIMESTAMP WITH TIME ZONE
// All dates will therefore come back here as localtime
// This forces dates to come back as UTC.
// See https://github.com/brianc/node-pg-types/blob/master/lib/textParsers.js
// Also http://stackoverflow.com/questions/20712291/use-node-postgres-to-get-postgres-timestamp-without-timezone-in-utc
types.setTypeParser(1082, function (stringVal) {
  return new Date(stringVal)
})

module.exports = Redshift

function Redshift (connString, s3credentials) {
  this.connString = connString
  this.s3credentials = s3credentials
  this.lineItemsTableName = process.env.LINE_ITEMS_TABLE_NAME || 'line_items'
  this.schema = process.env.SCHEMA || 'heroku'
}

// Execute a query, using the query pool
// Return a promise which resolves with the output of the query.
Redshift.prototype.executeQuery = function (query, transform) {
  var self = this
  log.debug('Executing query:')
  log.debug(query)
  return new Promise(function (resolve, reject) {
    pg.connect(self.connString, function (err, client, done) {
      if (err) throw err
      client.query(query, function (err, result) {
        if (err) throw err
        if (transform !== undefined) {
          result = transform(result)
        }
        done(client)
        return resolve(result)
      })
    })
  })
}

// Execute a query where the desired ouput is a single scalar value.
Redshift.prototype.getScalar = function (query, keyName) {
  let transform = function (result) {
    let rowzero = result.rows[0]
    if (keyName !== undefined) {
      return rowzero[keyName]
    } else {
      // return the value of the first key
      return rowzero[Object.keys(rowzero)[0]]
    }
  }
  return this.executeQuery(query, transform)
}

// Execute a query where the desired output is a count of affected rows
// (for example, DELETE FROM queries).
Redshift.prototype.getRowCount = function (query) {
  let transform = function (result) {
    return result.rowCount
  }
  return this.executeQuery(query, transform)
}

// Check that a table exists. Won't match on views or other table-like things.
Redshift.prototype.checkTableExists = function (tableName, schema) {
  let query = `
    SELECT EXISTS (
      SELECT 1
      FROM   pg_catalog.pg_class c
      JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE  n.nspname = '${schema}'
      AND    c.relname = '${tableName}'
      AND    c.relkind = 'r'    -- only tables
    ) as exists;`

  let transform = function (result) {
    return result.rows[0].exists === 't'
  }
  return this.executeQuery(query, transform)
}

// Determine whether a specific finalized month has already been imported.
Redshift.prototype.hasMonth = function (month) {
  let query = `
    SELECT COUNT(*)
    FROM ${this.schema}.${this.lineItemsTableName}
    WHERE statement_month = '${month.format('YYYY-MM-01')}';`
  return this.getScalar(query, 'count').then(function (count) {
    return (count > 0)
  })
}

// Import a finalized (full-month) DBR into Redshift.
// First, create a staging table and COPY FROM into that.
// Then, add the statement_month column, and copy it all to line_items.
// Then drop the staging table.
Redshift.prototype.importFullMonth = function (s3uri, month, pruneThresholdMonths) {
  const monthString = month.format('YYYY_MM')
  const monthDateString = month.format('YYYY-MM-01')
  const stagingTableName = `staging_${monthString}`
  let pruneQueryFragment = ''
  if (typeof pruneThresholdMonths === 'number') {
    // Delete statement months that are older than X months ago
    let pruneThresholdString = moment(month)
      .subtract(pruneThresholdMonths, 'months')
      .format('YYYY-MM-01')
    pruneQueryFragment = `DELETE FROM ${this.schema}.line_items WHERE statement_month <= '${pruneThresholdString}'::DATE;`
  }
  // Normally, creating the staging table would look like:
  //   CREATE TABLE ${stagingTableName} (LIKE line_items)
  // However you can't alter the staging table to drop statement_month because
  // it is specified as the SORTKEY, and that can't be touched in existing
  // tables. So, we create the staging table from scratch.
  let query = `
    BEGIN;
      CREATE TABLE IF NOT EXISTS ${this.schema}.${stagingTableName} (
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

      COPY ${this.schema}.${stagingTableName}
        FROM '${s3uri}'
        CREDENTIALS 'aws_access_key_id=${this.s3credentials.key};aws_secret_access_key=${this.s3credentials.secret}'
        GZIP CSV IGNOREHEADER 1;
      ALTER TABLE ${this.schema}.${stagingTableName} ADD COLUMN statement_month DATE DEFAULT '${monthDateString}';
      DELETE FROM ${this.schema}.${this.lineItemsTableName} WHERE statement_month = '${monthDateString}';
      INSERT INTO ${this.schema}.${this.lineItemsTableName} SELECT * FROM ${this.schema}.${stagingTableName};
      DROP TABLE ${this.schema}.${stagingTableName};
      ${pruneQueryFragment}
    COMMIT;
  `

  return this.executeQuery(query)
}

// Import the month-to-date DBR into the month_to_date table, clobbering
// whatever was already there.
Redshift.prototype.importMonthToDate = function (s3uri) {
  let self = this
  let truncateQuery = `TRUNCATE ${self.schema}.month_to_date;`
  return this.executeQuery(truncateQuery).then(function () {
    log.debug('Month to date table truncated. Importing...')
    let query = `
      COPY ${self.schema}.month_to_date
        FROM '${s3uri}'
        CREDENTIALS 'aws_access_key_id=${self.s3credentials.key};aws_secret_access_key=${self.s3credentials.secret}'
        GZIP CSV IGNOREHEADER 1;
    `
    return self.executeQuery(query)
  })
}

// Vacuum the database.
Redshift.prototype.vacuum = function (tableName) {
  let query = `VACUUM ${(this.schema) + '.' + tableName || ''};`
  return this.executeQuery(query)
}
