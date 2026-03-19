-- SQL command to create the production jobs table.
-- This schema is designed to store detailed information about each job
-- executed within the sPHENIX production system.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'prodstate') THEN
        CREATE TYPE prodstate AS ENUM (
               'submitting',       -- job is being submitted
               'submitted',        -- job is submitted
               'started',          -- job has started on the worker node
               'running',          -- job is running
               'held',             -- job is held
               'evicted',          -- job has recieved a bad signal
               'failed',           -- job ended with nonzero status code
               'finished'          -- job has finished
        );
    END IF;
END$$;

CREATE TABLE production_jobs (
    id                    SERIAL                  PRIMARY KEY,
    ClusterId             BIGINT,
    ProcId                INT,
    rulename              TEXT                    NOT NULL,
    tag                   TEXT                    NOT NULL,
    dataset               TEXT                    NOT NULL,
    dsttype               TEXT                    NOT NULL,
    filename              TEXT,
    run                   INT                     NOT NULL,
    segment               INT,
    status                prodstate,
    submitted             TIMESTAMP WITH TIME ZONE  DEFAULT CURRENT_TIMESTAMP,
    started               TIMESTAMP WITH TIME ZONE,
    finished              TIMESTAMP WITH TIME ZONE,
    RemoteUserCpu         FLOAT,
    RemoteSysCpu          FLOAT,
    MemoryUsage           INT,
    DiskUsage             INT,
    ExitCode              INT,
    submission_host       TEXT,
    execution_node        TEXT,
    log                   TEXT,
    err                   TEXT,
    out                   TEXT,
    intriplet             TEXT,
    indsttype_str         TEXT,
    xferslots             INT,
    request_memory        INT[],
    request_disk          INT,
    request_cpus          INT,
    neventsper            INT
);

-- Add comments to the table and columns to explain their purpose.
COMMENT ON TABLE production_jobs IS 'Table to store information about individual production jobs.';
COMMENT ON COLUMN production_jobs.id IS 'Unique identifier for each job record in this table.';
COMMENT ON COLUMN production_jobs.ClusterId IS 'The cluster ID assigned by the batch system (e.g., HTCondor).';
COMMENT ON COLUMN production_jobs.ProcId IS 'The process ID within the cluster assigned by the batch system.';
COMMENT ON COLUMN production_jobs.rulename IS 'The name of the production rule from the YAML configuration.';
COMMENT ON COLUMN production_jobs.tag IS 'The output triplet (e.g., new_2025p000_v000) for the production.';
COMMENT ON COLUMN production_jobs.dataset IS 'The dataset identifier (e.g., run3auau or run3cosmics).';
COMMENT ON COLUMN production_jobs.dsttype IS 'The type of data being produced (e.g., DST_CALOFITTING).';
COMMENT ON COLUMN production_jobs.filename IS 'The name of the output file (dstfile).';
COMMENT ON COLUMN production_jobs.run IS 'The run number being processed.';
COMMENT ON COLUMN production_jobs.segment IS 'The segment number of the data file being processed.';
COMMENT ON COLUMN production_jobs.status IS 'The production status of the job.';
COMMENT ON COLUMN production_jobs.submitted IS 'Timestamp when the job was submitted to the batch system. From Condor''s QDate attribute.';
COMMENT ON COLUMN production_jobs.started IS 'Timestamp when the job execution began. From Condor''s JobStartDate attribute.';
COMMENT ON COLUMN production_jobs.finished IS 'Timestamp when the job execution finished. From Condor''s CompletionDate attribute.';
COMMENT ON COLUMN production_jobs.RemoteUserCpu IS 'User CPU time used by the job, in seconds. From Condor''s RemoteUserCpu attribute.';
COMMENT ON COLUMN production_jobs.RemoteSysCpu IS 'System CPU time used by the job, in seconds. From Condor''s RemoteSysCpu attribute.';
COMMENT ON COLUMN production_jobs.MemoryUsage IS 'Peak memory usage of the job, in MB. From Condor''s MemoryUsage attribute.';
COMMENT ON COLUMN production_jobs.DiskUsage IS 'Peak disk usage of the job, in KB. From Condor''s DiskUsage attribute.';
COMMENT ON COLUMN production_jobs.ExitCode IS 'The exit code of the job process. From Condor''s ExitCode attribute.';
COMMENT ON COLUMN production_jobs.submission_host IS 'The hostname of the machine where the job was submitted from.';
COMMENT ON COLUMN production_jobs.execution_node IS 'The hostname of the node where the job ran. From Condor''s RemoteHost attribute.';
COMMENT ON COLUMN production_jobs.log IS 'Path to the standard output log file.';
COMMENT ON COLUMN production_jobs.err IS 'Path to the standard error log file.';
COMMENT ON COLUMN production_jobs.out IS 'Path to the primary output file produced by the job.';
COMMENT ON COLUMN production_jobs.intriplet IS 'For input data provenance.';
COMMENT ON COLUMN production_jobs.indsttype_str IS 'For input data provenance.';
COMMENT ON COLUMN production_jobs.xferslots IS 'To log requested resources for debugging and planning.';
COMMENT ON COLUMN production_jobs.request_memory IS 'To log requested resources for debugging and planning. Array of integers in MB.';
COMMENT ON COLUMN production_jobs.request_disk IS 'To log requested resources for debugging and planning.';
COMMENT ON COLUMN production_jobs.request_cpus IS 'To log requested resources for debugging and planning.';
COMMENT ON COLUMN production_jobs.neventsper IS 'To record the number of events processed per job.';

-- Create indexes on frequently queried columns for better performance.
CREATE INDEX ON production_jobs (run);
CREATE INDEX ON production_jobs (status);
CREATE INDEX ON production_jobs (rulename);
CREATE INDEX ON production_jobs (tag);

-- A composite unique index can be useful for queries that filter on both cluster and process id.
CREATE UNIQUE INDEX production_jobs_cluster_process_id_idx ON production_jobs (ClusterId, ProcId);
