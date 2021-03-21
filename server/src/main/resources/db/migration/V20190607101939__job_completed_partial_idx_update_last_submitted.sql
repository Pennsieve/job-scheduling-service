CREATE INDEX idx_job_completed ON jobs (id) WHERE NOT completed;

UPDATE etl.jobs j_update
SET submitted_at = last_submission.sent_at
FROM (
    -- GET LAST SUBMISSION
    SELECT j.id, max(js.sent_at) as sent_at
    FROM etl.jobs j INNER JOIN etl.job_state js ON j.id = js.job_id
    WHERE js.state = 'SUBMITTED'
    GROUP BY j.id
        ) last_submission
WHERE last_submission.id = j_update.id
    ;

DROP FUNCTION organization_last_submitted() CASCADE;

CREATE FUNCTION last_submitted()
RETURNS TRIGGER AS $last_submitted$
DECLARE _organization_id integer;
BEGIN
  SELECT j.organization_id
  INTO _organization_id
  FROM etl.jobs j
  WHERE j.id = NEW.job_id;

  UPDATE etl.organization_quota
  SET submitted_at = now()
  WHERE organization_id = _organization_id;

  UPDATE etl.jobs
  SET submitted_at = now()
  WHERE id = NEW.job_id;

  RETURN null;
END;
$last_submitted$ language plpgsql;

CREATE TRIGGER record_last_submitted
  AFTER INSERT ON etl.job_state
  FOR EACH ROW
  WHEN (NEW.state = 'SUBMITTED')
  EXECUTE PROCEDURE last_submitted();
