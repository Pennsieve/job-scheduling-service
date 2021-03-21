ALTER TABLE organization_quota ADD COLUMN submitted_at TIMESTAMP;

UPDATE organization_quota o
SET submitted_at = last_submission.sent_at
FROM (
       -- GET LAST SUBMISSION
       SELECT j.organization_id, max(js.sent_at) as sent_at
       FROM jobs j INNER JOIN job_state js ON j.id = js.job_id
       WHERE js.state = 'SUBMITTED'
       GROUP BY j.organization_id
   ) last_submission
WHERE last_submission.organization_id = o.organization_id
    ;
