CREATE TABLE IF NOT EXISTS ri_leases (
  account_id TEXT,
  payer_account_id TEXT,
  start_date DATE,
  end_date DATE,
  lease_term TEXT,
  az TEXT,
  instance_type TEXT,
  os TEXT,
  utilization TEXT,
  tenancy TEXT,
  fixed_price NUMERIC(11, 6),
  usage_price NUMERIC(8, 6),
  instance_count INT,
  lease_id TEXT,
  subscription_id TEXT,
  state TEXT,
  PRIMARY KEY(subscription_id)
);
