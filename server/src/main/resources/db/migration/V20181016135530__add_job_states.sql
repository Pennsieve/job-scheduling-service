CREATE TABLE job_state (
  id UUID PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES jobs(id) ON UPDATE RESTRICT ON DELETE CASCADE,
  state VARCHAR(40) NOT NULL,
  sent_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION org_user_last_submitted()
RETURNS TRIGGER AS $org_user_last$
DECLARE _organization_id integer;
DECLARE _user_id integer;
BEGIN
  SELECT j.organization_id, j.user_id
  INTO _organization_id, _user_id
  FROM etl.jobs j
  WHERE j.id = NEW.job_id;

  UPDATE etl.organization_quota
    SET submitted_at = now()
  WHERE organization_id = _organization_id;
  UPDATE etl.user_submission
    SET submitted_at = now()
  WHERE user_id = _user_id;
  RETURN null;
END;
$org_user_last$ language plpgsql;

CREATE INDEX idx_jobState_jobId ON job_state (job_id);
DROP TRIGGER record_last_submitted_organization ON jobs;
CREATE TRIGGER record_last_submitted_organization
  AFTER INSERT ON job_state
  FOR EACH ROW
  WHEN (NEW.state = 'SUBMITTED')
  EXECUTE PROCEDURE org_user_last_submitted();

ALTER TABLE jobs DROP COLUMN state;
