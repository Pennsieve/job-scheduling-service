CREATE FUNCTION org_user_last_submitted()
RETURNS TRIGGER AS $org_user_last$
BEGIN
  UPDATE etl.organization_quota
    SET submitted_at = now()
  WHERE organization_id = NEW.organization_id;
  UPDATE etl.user_submission
    SET submitted_at = now()
  WHERE user_id = NEW.user_id;
  RETURN null;
END;
$org_user_last$ language plpgsql;

CREATE TRIGGER record_last_submitted_organization
  AFTER UPDATE ON jobs
  FOR EACH ROW
  WHEN (NEW.state = 'SUBMITTED')
  EXECUTE PROCEDURE org_user_last_submitted();