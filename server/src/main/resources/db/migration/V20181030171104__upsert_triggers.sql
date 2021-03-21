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

  INSERT INTO etl.user_submission
      (user_id, submitted_at)
      VALUES (_user_id, now())
      ON CONFLICT (user_id) DO
  UPDATE SET submitted_at = now();

  RETURN null;
END;
$org_user_last$ language plpgsql;
