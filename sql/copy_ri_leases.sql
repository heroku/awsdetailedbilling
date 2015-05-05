COPY ri_leases
FROM 's3://heroku-detailed-billing-staging/a_lease_report.csv'
CREDENTIALS 'aws_access_key_id=FOO;aws_secret_access_key=BAR'
CSV
IGNOREHEADER 1
DATEFORMAT 'MM/DD/YYYY';
