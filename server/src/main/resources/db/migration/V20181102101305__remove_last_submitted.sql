ALTER TABLE etl.organization_quota DROP COLUMN submitted_at;
DROP TABLE etl.user_submission;
DROP TRIGGER record_last_submitted_organization ON job_state;
DROP FUNCTION org_user_last_submitted();
