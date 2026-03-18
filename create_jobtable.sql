-- Create a placeholder enum type for status
CREATE TYPE job_status_enum AS ENUM ('submitted', 'running', 'finished', 'failed');

CREATE TABLE jobstatus (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basefilename TEXT,
    dataset TEXT,
    dsttype TEXT,
    tag TEXT,
    runnumber INT,
    status job_status_enum,
    started TIMESTAMP,
    running TIMESTAMP,
    ended TIMESTAMP
);

CREATE INDEX idx_jobstatus_basefilename ON jobstatus (basefilename);
CREATE INDEX idx_jobstatus_dsttype ON jobstatus (dsttype);
CREATE INDEX idx_jobstatus_tag ON jobstatus (tag);
CREATE INDEX idx_jobstatus_runnumber ON jobstatus (runnumber);
CREATE INDEX idx_jobstatus_status ON jobstatus (status);

-- Combined index for hierarchical lookups (Production -> Type -> Tag -> Run)
-- This replaces the need for a standalone dataset index and speeds up specific job lookups
CREATE INDEX idx_jobstatus_full_production ON jobstatus (dataset, dsttype, tag, runnumber);