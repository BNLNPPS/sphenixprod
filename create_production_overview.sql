-- Table and trigger for the production overview dashboard.
-- Rows are auto-registered on first INSERT into production_jobs.
-- Curators flip `active = true` to make a campaign visible on the dashboard.

CREATE TABLE IF NOT EXISTS production_overview (
    id               SERIAL                   PRIMARY KEY,
    tag              TEXT                     NOT NULL,
    production_type  TEXT                     NOT NULL,  -- 'tracking' or 'calo'
    active           BOOLEAN                  NOT NULL DEFAULT false,
    display_name     TEXT,
    priority         INT                      NOT NULL DEFAULT 0,
    started_at       TIMESTAMP WITH TIME ZONE,
    expected_end     TIMESTAMP WITH TIME ZONE,
    notes            TEXT,
    registered_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tag, production_type),
    CHECK (production_type IN ('tracking', 'calo'))
);

COMMENT ON TABLE  production_overview                  IS 'Curator-managed registry of production campaigns shown on the monitoring dashboard.';
COMMENT ON COLUMN production_overview.tag              IS 'Matches tag in production_jobs.';
COMMENT ON COLUMN production_overview.production_type  IS 'tracking or calo — one tag may have both.';
COMMENT ON COLUMN production_overview.active           IS 'Set true by a curator to show this campaign on the dashboard.';
COMMENT ON COLUMN production_overview.display_name     IS 'Optional human-readable label for dashboard panel titles.';
COMMENT ON COLUMN production_overview.priority         IS 'Display order; lower numbers appear first.';
COMMENT ON COLUMN production_overview.started_at       IS 'Campaign start time for ETA / progress panels.';
COMMENT ON COLUMN production_overview.expected_end     IS 'Expected completion time for ETA / progress panels.';
COMMENT ON COLUMN production_overview.notes            IS 'Free-text curator annotation surfaced as a dashboard tooltip.';
COMMENT ON COLUMN production_overview.registered_at    IS 'Timestamp of the first job that triggered auto-registration.';

CREATE INDEX IF NOT EXISTS production_overview_active_type_idx
    ON production_overview (active, production_type);

-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION auto_register_production_overview()
RETURNS TRIGGER AS $$
DECLARE
    v_prod_type TEXT;
BEGIN
    v_prod_type := CASE
        WHEN NEW.dsttype LIKE 'DST_STREAMING_EVENT%' OR NEW.dsttype LIKE 'DST_TRKR_%'
            THEN 'tracking'
        WHEN NEW.dsttype LIKE 'DST_TRIGGERED_EVENT%'
          OR NEW.dsttype IN ('DST_CALOFITTING', 'DST_CALO', 'DST_JETS', 'DST_JETCALO')
            THEN 'calo'
        ELSE NULL
    END;

    IF v_prod_type IS NULL THEN
        RAISE NOTICE
            'auto_register_production_overview: unrecognised dsttype "%" (tag=%, rulename=%) — skipping registration',
            NEW.dsttype, NEW.tag, NEW.rulename;
        RETURN NEW;
    END IF;

    INSERT INTO production_overview (tag, production_type)
    VALUES (NEW.tag, v_prod_type)
    ON CONFLICT (tag, production_type) DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_auto_register_overview
    AFTER INSERT ON production_jobs
    FOR EACH ROW EXECUTE FUNCTION auto_register_production_overview();
