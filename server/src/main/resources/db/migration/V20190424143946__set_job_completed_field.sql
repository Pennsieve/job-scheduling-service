UPDATE jobs job_update
SET completed = true
FROM (
    -- CHECK FOR IF TERMINAL STATES EXIST
    SELECT job_id, current_state FROM (
        SELECT DISTINCT
            j.id as job_id,
            LAST_VALUE(js.state) OVER(
                PARTITION BY js.job_id ORDER BY js.sent_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS current_state
        FROM jobs j
            INNER JOIN job_state js ON j.id = js.job_id
            ) with_current_state
        ) job_current_state
WHERE job_current_state.current_state IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'NOT_PROCESSING', 'LOST')
    AND job_current_state.job_id = job_update.id
    ;
