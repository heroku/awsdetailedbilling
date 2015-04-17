/* jshint esnext: true, node: true */
'use strict';

var ArgumentParser = require('argparse').ArgumentParser;

module.exports = Parser;

function Parser(opts) {
  let parser = new ArgumentParser(opts);

  parser.addArgument(
    ['--source-bucket'], {
      help: 'The S3 bucket which contains the detailed billing reports. Defaults to the environment variable "DBR_BUCKET".',
      defaultValue: process.env.SOURCE_BUCKET
    }
  );

  parser.addArgument(
    ['--source-key'], {
      help: 'An AWS access key ID with permissions to access the source DBR bucket. Defaults to the environment variable "SOURCE_AWS_KEY", then to "AWS_KEY".',
      defaultValue: process.env.SOURCE_AWS_KEY || process.env.AWS_KEY
    }
  );

  parser.addArgument(
    ['--source-secret'], {
      help: 'An AWS access key secret with permissions to access the source DBR bucket. Defaults to the environment variable "SOURCE_AWS_SECRET", then to "AWS_SECRET".',
      defaultValue: process.env.SOURCE_AWS_SECRET || process.env.AWS_SECRET
    }
  );

  parser.addArgument(
    ['--staging-bucket'], {
      help: 'The S3 bucket which serves as a staging area for loading detailed billing reports. Defaults to the environment variable "STAGING_BUCKET".',
      defaultValue: process.env.STAGING_BUCKET
    }
  );

  parser.addArgument(
    ['--staging-key'], {
      help: 'An AWS access key ID with permissions to access the staging DBR bucket. Defaults to the environment variable "STAGING_AWS_KEY", then to "AWS_KEY".',
      defaultValue: process.env.STAGING_AWS_KEY || process.env.AWS_KEY
    }
  );

  parser.addArgument(
    ['--staging-secret'], {
      help: 'An AWS access key secret with permissions to access the staging DBR bucket. Defaults to the environment variable "STAGING_AWS_KEY", then to "AWS_KEY".',
      defaultValue: process.env.STAGING_AWS_SECRET || process.env.AWS_SECRET
    }
  );

  parser.addArgument(
    ['--redshift-uri'], {
      help: 'The redshift connection string, in URI form',
      defaultValue: process.env.REDSHIFT_URI
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

  parser.addArgument(
    ['-d', '--debug'], {
      action: 'storeConst',
      dest: 'debug',
      help: 'Turn on debugging output.',
      constant: true
    }
  );

  return parser;
}
