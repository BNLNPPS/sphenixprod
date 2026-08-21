-- SQL draft for migrating generate_report.py CSV output into the production DB.
-- One row represents one run-level report snapshot for one production rule.
-- Intended initial source: generate_report.py for raw/event-combiner rules.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'run_report_status') THEN
        CREATE TYPE run_report_status AS ENUM (
            'complete',       -- no missing events and no other report concerns
            'questionable',   -- no missing events, but one or more non-event concerns exist
            'incomplete'      -- missing events remain after expected bookkeeping allowances
        );
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS production_run_reports (
    id                              SERIAL                   PRIMARY KEY,

    rule_name                       TEXT                     NOT NULL,
    runnumber                       INT                      NOT NULL,
    tag                             TEXT,
    dataset                         TEXT,
    dsttype                         TEXT,

    possible_daqhosts               INT                      NOT NULL DEFAULT 0,
    total_daqhosts                  INT                      NOT NULL DEFAULT 0,
    missing_daqhosts                INT                      NOT NULL DEFAULT 0,

    possible_segments               INT                      NOT NULL DEFAULT 0,
    total_segments                  INT                      NOT NULL DEFAULT 0,
    missing_segments                INT                      NOT NULL DEFAULT 0,

    possible_events                 BIGINT                   NOT NULL DEFAULT 0,
    total_events                    BIGINT                   NOT NULL DEFAULT 0,
    expected_skipped_events         BIGINT                   NOT NULL DEFAULT 0,
    missing_events                  BIGINT                   NOT NULL DEFAULT 0,

    error_codes                     INT[]                    NOT NULL DEFAULT '{}',
    incomplete_reasons              TEXT[]                   NOT NULL DEFAULT '{}',
    status                          run_report_status        NOT NULL,

    report_source                   TEXT                     NOT NULL DEFAULT 'generate_report.py',
    generated_at                    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    prod_as_of                      TIMESTAMP WITH TIME ZONE,
    raw_as_of                       TIMESTAMP WITH TIME ZONE,
    datasets_as_of                  TIMESTAMP WITH TIME ZONE,
    files_as_of                     TIMESTAMP WITH TIME ZONE,
    first_created_at                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at                 TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    notes                           TEXT,

    UNIQUE (rule_name, runnumber, generated_at),

    CHECK (possible_daqhosts >= 0),
    CHECK (total_daqhosts >= 0),
    CHECK (missing_daqhosts >= 0),
    CHECK (possible_segments >= 0),
    CHECK (total_segments >= 0),
    CHECK (missing_segments >= 0),
    CHECK (possible_events >= 0),
    CHECK (total_events >= 0),
    CHECK (expected_skipped_events >= 0),
    CHECK (missing_events >= 0)
);

COMMENT ON TABLE production_run_reports IS 'Run-level production completeness reports, initially mirroring generate_report.py CSV output.';
COMMENT ON COLUMN production_run_reports.id IS 'Unique identifier for this report row.';
COMMENT ON COLUMN production_run_reports.rule_name IS 'Production rule name from the YAML configuration; matches generate_report.py --rulename.';
COMMENT ON COLUMN production_run_reports.runnumber IS 'DAQ run number summarized by this row.';
COMMENT ON COLUMN production_run_reports.tag IS 'Output production triplet, e.g. ana561_2025p000_v000; nullable in the draft until the writer supplies it.';
COMMENT ON COLUMN production_run_reports.dataset IS 'Dataset identifier, e.g. run3auau, run3pp, or run3oo.';
COMMENT ON COLUMN production_run_reports.dsttype IS 'Output DST type or template represented by this report row.';
COMMENT ON COLUMN production_run_reports.possible_daqhosts IS 'Number of daqhosts expected for this run from the raw DB universe.';
COMMENT ON COLUMN production_run_reports.total_daqhosts IS 'Number of expected daqhosts with matching FileCatalog output coverage.';
COMMENT ON COLUMN production_run_reports.missing_daqhosts IS 'Expected daqhosts without matching FileCatalog output coverage.';
COMMENT ON COLUMN production_run_reports.possible_segments IS 'Expected number of output segments per daqhost, usually ceil(eventsinrun / neventsper).';
COMMENT ON COLUMN production_run_reports.total_segments IS 'Number of segments considered complete per daqhost after applying report logic.';
COMMENT ON COLUMN production_run_reports.missing_segments IS 'Expected per-daqhost segments not considered complete.';
COMMENT ON COLUMN production_run_reports.possible_events IS 'Expected DST-entry event total across daqhosts. One physical event contributes once per daqhost.';
COMMENT ON COLUMN production_run_reports.total_events IS 'Observed FileCatalog event total across daqhost-specific outputs.';
COMMENT ON COLUMN production_run_reports.expected_skipped_events IS 'Known bookkeeping allowance, currently two skipped events per possible daqhost for event-combiner output.';
COMMENT ON COLUMN production_run_reports.missing_events IS 'Event deficit after subtracting expected_skipped_events; this is the hard gate for incomplete status.';
COMMENT ON COLUMN production_run_reports.error_codes IS 'Distinct nonzero production_jobs ExitCode values encountered for this run.';
COMMENT ON COLUMN production_run_reports.incomplete_reasons IS 'Machine-readable concern labels such as missing_daqhosts, missing_segments, low_event_ratio, or error_codes.';
COMMENT ON COLUMN production_run_reports.status IS 'complete, questionable, or incomplete. Incomplete requires missing_events > 0.';
COMMENT ON COLUMN production_run_reports.report_source IS 'Tool or process that generated this row.';
COMMENT ON COLUMN production_run_reports.generated_at IS 'Timestamp when this report content was generated by the report producer.';
COMMENT ON COLUMN production_run_reports.prod_as_of IS 'Optional watermark for production DB state, currently production_jobs rows used for nonzero ExitCode reporting.';
COMMENT ON COLUMN production_run_reports.raw_as_of IS 'Optional watermark for raw DB datasets rows used for expected daqhost and raw availability checks.';
COMMENT ON COLUMN production_run_reports.datasets_as_of IS 'Optional watermark for FileCatalog datasets rows used for output dsttype and lastevent coverage.';
COMMENT ON COLUMN production_run_reports.files_as_of IS 'Optional watermark for FileCatalog files rows; currently reserved for future report logic.';
COMMENT ON COLUMN production_run_reports.first_created_at IS 'Timestamp when this database row was first inserted.';
COMMENT ON COLUMN production_run_reports.last_updated_at IS 'Timestamp when this database row was last updated by an upsert or refresh.';
COMMENT ON COLUMN production_run_reports.notes IS 'Free-text operator notes or migration comments.';

CREATE INDEX IF NOT EXISTS production_run_reports_rule_run_idx
    ON production_run_reports (rule_name, runnumber);

CREATE INDEX IF NOT EXISTS production_run_reports_status_idx
    ON production_run_reports (status);

CREATE INDEX IF NOT EXISTS production_run_reports_tag_dataset_dsttype_idx
    ON production_run_reports (tag, dataset, dsttype);
