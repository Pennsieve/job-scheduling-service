CREATE SCHEMA etl_events;

DROP TABLE IF EXISTS etl_events.journal;

CREATE TABLE IF NOT EXISTS etl_events.journal (
  ordering BIGSERIAL,
  persistence_id VARCHAR(255) NOT NULL,
  sequence_number BIGINT NOT NULL,
  deleted BOOLEAN DEFAULT FALSE,
  tags VARCHAR(255) DEFAULT NULL,
  message BYTEA NOT NULL,
  PRIMARY KEY(persistence_id, sequence_number)
);

CREATE UNIQUE INDEX journal_ordering_idx ON etl_events.journal(ordering);

DROP TABLE IF EXISTS etl_events.snapshot;

CREATE TABLE IF NOT EXISTS etl_events.snapshot (
  persistence_id VARCHAR(255) NOT NULL,
  sequence_number BIGINT NOT NULL,
  created BIGINT NOT NULL,
  snapshot BYTEA NOT NULL,
  PRIMARY KEY(persistence_id, sequence_number)
);
