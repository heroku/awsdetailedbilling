# awsdetailedbilling

Loads AWS detailed billing reports into a redshift cluster.

[![js-standard-style](https://cdn.rawgit.com/feross/standard/master/badge.svg)](https://github.com/feross/standard)


# Setup

Still a manual process for now:

1. Create a Redshift cluster.
2. Once the cluster is alive, connect with your favorite postgres client and create the `line_items` and `month_to_date` tables. The SQL for creating each are in the `sql/` subdirectory.


## Configuration:

Set these environment variables. Some of them may be overridden at runtime with command-line switches, run the relevant script with `--help` for more details.

- `SOURCE_BUCKET`: the S3 bucket where DBRs are deposited by Amazon.
- `STAGING_BUCKET`: the S3 bucket into which pre-processed DBRs are staged before importing to redshift.
- `AWS_KEY` *or* `SOURCE_AWS_KEY` and `STAGING_AWS_KEY`: the AWS access key ID credential for accessing S3. If the same credentials are used for both the source and staging buckets, you can just set `AWS_KEY`. If separate credentials are neccessary, you can specify `SOURCE_AWS_KEY` *and* `STAGING_AWS_KEY` instead.
- `AWS_SECRET` *or* `SOURCE_AWS_SECRET` and `STAGING_AWS_SECRET`: Same as `AWS_KEY`, but for your AWS access key secret.
- `REDSHIFT_URI`: a connection URI for redshift. Should include credentials, like the form `postgres://myUser:s0mep4ssword@hostname:port/dbname`
- `ROLLBAR_TOKEN`: a token for error reporting to Rollbar.
- `ROLLBAR_ENVIRONMENT`: an environment name for error reporting to Rollbar.


## Usage

There are two scripts: `import_finalized.js` and `import_month_to_date.js`. Both are intended to be run on a daily schedule, preferably at night. Run duration is largely dependent on the size of your DBRs; for large DBRs runs of a few hours are common.

Invoke either with `--help` for invocation instructions.


#### `import_finalized.js`

This script imports "finalized" DBRs — specifically, the DBR for the previous month according to UTC.

The script first checks to see if there's a finalized DBR which hasn't been imported yet. If there is no new finalized DBR, the script terminates immediately. Once a month, when a new finalized DBR appears, the script will download, unzip, gzip, stage, and import the DBR into a temporary table named `staging_YYYY_MM`. Once that process is complete, it adds a `statement_month` column with the relevant month, copies the entire staging table into `line_items`, drops the staging table, and `VACUUM`s the line_items table.

#### `import_month_to_date.js`

This script imports "month-to-date" DBRs, which contain "estimated" billing data but are not 100% accurate. Upon every import, the current month's DBR is downloaded, unzipped, gzipped, and staged. The `month_to_date` table is emptied by means of  [TRUNCATE](http://docs.aws.amazon.com/redshift/latest/dg/r_TRUNCATE.html) (eliminating the need for an interim VACUUM), and the staged DBR is imported, followed by a VACUUM.

### Usage tips

You can run these on your local machine, but unless you live very nearby the AWS datacenters where your source and staging S3 buckets are located, you'll have better performance running them on Heroku.

Use PX dynos for invoking either script; smaller dyno types lack the memory and storage to get the job done.

Here's a sample invocation:

`heroku run -s PX "iojs import_finalized.js"

If you want to run it without fear of laptop disconnections, you can run the process in detached mode:

`heroku run:detached -s PX "iojs import_finalized.js"`

You can track progress by running `heroku logs -t`

## Future improvements

- One-off month imports
- Heroku button!

## Meta

License: MIT. See LICENSE.txt.

Questions? Comments? Hit up tools@heroku.com.
