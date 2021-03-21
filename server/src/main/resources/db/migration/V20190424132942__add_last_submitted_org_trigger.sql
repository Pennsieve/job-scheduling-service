CREATE OR REPLACE FUNCTION organization_last_submitted()
RETURNS TRIGGER AS $organization_last$
DECLARE _organization_id integer;
BEGIN
  SELECT j.organization_id
  INTO _organization_id
  FROM etl.jobs j
  WHERE j.id = NEW.job_id;

  UPDATE etl.organization_quota
  SET submitted_at = now()
  WHERE organization_id = _organization_id;

  RETURN null;
END;
$organization_last$ language plpgsql;

CREATE TRIGGER record_last_submitted_organization
  AFTER INSERT ON job_state
  FOR EACH ROW
  WHEN (NEW.state = 'SUBMITTED')
  EXECUTE PROCEDURE organization_last_submitted();
