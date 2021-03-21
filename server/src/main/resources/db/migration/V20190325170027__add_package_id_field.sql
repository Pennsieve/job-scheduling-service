BEGIN;
ALTER TABLE payloads ADD COLUMN package_id integer;

UPDATE payloads
SET package_id = (payload->>'packageId')::int
WHERE payload->>'packageId' IS NOT NULL;
END;