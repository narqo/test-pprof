CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS timescaledb;

DROP TABLE IF EXISTS profile_pprof_samples_cpu;

CREATE TABLE profile_pprof_samples_cpu(
  build_id      TEXT NOT NULL,
  token         TEXT NOT NULL,
  locations     INTEGER[],
  created_at    TIMESTAMPTZ NOT NULL,
  value_cpu     INTEGER,
  value_nanos   INTEGER
);
SELECT create_hypertable('profile_pprof_samples_cpu', 'created_at', create_default_indexes=>FALSE);

CREATE INDEX ON profile_pprof_samples_cpu (build_id, token, created_at DESC);

DROP TABLE IF EXISTS profile_pprof_locations;

CREATE TABLE profile_pprof_locations(
  location_id SERIAL PRIMARY KEY,
  func        TEXT NOT NULL,
  file_name   TEXT NOT NULL,
  line        INTEGER NOT NULL,

  UNIQUE (func, file_name, line)
);

CREATE INDEX ON profile_pprof_locations (func, file_name, line);

DROP TABLE IF EXISTS services;

CREATE TABLE services(
  build_id TEXT NOT NULL,
  token    TEXT NOT NULL,
  name     TEXT,
  labels   hstore,

  PRIMARY KEY (build_id, token)
);

INSERT INTO services(build_id, token, name, labels) VALUES
('123', 'fra.1', 'adjust_server', 'dc => fra, host => "backend-1", version => "1.0"'),
('123', 'fra.2', 'adjust_server', 'dc => fra, host => "backend-1", version => "1.0"'),
('123', 'ams.2', 'adjust_server', 'dc => ams, host => "ams-backend-1.adjust.com", version => "1.0"');
