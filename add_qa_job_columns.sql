-- Apply to existing production_jobs before submitting rules with qadbtag.
ALTER TABLE production_jobs ADD COLUMN IF NOT EXISTS qa_tag TEXT;
ALTER TABLE production_jobs ADD COLUMN IF NOT EXISTS qa_filename TEXT;
ALTER TABLE production_jobs ADD COLUMN IF NOT EXISTS qa_status TEXT;
ALTER TABLE production_jobs ADD COLUMN IF NOT EXISTS qa_exit_code INT;
ALTER TABLE production_jobs DROP CONSTRAINT IF EXISTS production_jobs_qa_status_check;
ALTER TABLE production_jobs ADD CONSTRAINT production_jobs_qa_status_check
    CHECK (qa_status IN ('pending', 'succeeded', 'failed', 'missing')) NOT VALID;

-- Run separately during a suitable window if a validated constraint is desired:
-- ALTER TABLE production_jobs VALIDATE CONSTRAINT production_jobs_qa_status_check;
