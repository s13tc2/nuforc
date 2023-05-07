#!/bin/bash

set -eux

# Create database
psql -v ON_ERROR_STOP=1 <<-EOSQL
  CREATE DATABASE nuforc;
EOSQL

# Create table
psql -v ON_ERROR_STOP=1 nuforc <<-EOSQL
  CREATE TABLE IF NOT EXISTS ufosightings (
    summary             TEXT,
    country             VARCHAR(100),
    city                VARCHAR(100),
    state               VARCHAR(50),
    date_time           TIMESTAMP,
    shape               VARCHAR(20),    
    duration            VARCHAR(50),
    stats               TEXT,
    report_link         VARCHAR(100),
    text                TEXT,
    posted              TIMESTAMP,
    city_latitude       FLOAT8,
    city_longitude      FLOAT8
  );
EOSQL

time psql -v ON_ERROR_STOP=1 nuforc <<-EOSQL
    COPY ufosightings
    FROM '/tmp/nuforc_reports.csv' DELIMITER ',' CSV HEADER;
EOSQL


psql -v ON_ERROR_STOP=1 <<-EOSQL
    CREATE USER ufo WITH PASSWORD 'uap';
    GRANT ALL PRIVILEGES ON DATABASE nuforc TO ufo;
    \c nuforc;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ufo;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ufo;
EOSQL

pg_ctl stop

