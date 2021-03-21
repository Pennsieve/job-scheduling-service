CREATE TABLE organization_quota (
  organization_id INTEGER PRIMARY KEY,
  slots_allowed INTEGER NOT NULL,
  submitted_at TIMESTAMP
);

CREATE TABLE user_submission (
  user_id INTEGER PRIMARY KEY,
  submitted_at TIMESTAMP
);