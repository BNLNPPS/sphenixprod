# Production Monitoring Dashboard — Design Notes

## Goal

A Grafana dashboard for monitoring ongoing sPHENIX production campaigns, showing job progress, resource usage, and failure rates. The primary audience is production curators who need a live view of what is running and how healthy it is, as well as users who just need a progress view.

---

## Fundamental splits

### Tracking vs. calo

There is a fundamental physics-motivated separation between tracking and calorimeter productions. A single `tag` (campaign identifier) can and does contain both tracking and calo jobs, so the split is a property of the **(tag, production_type) pair**, not the tag alone.

The production type is inferred unambiguously from `dsttype`:

| production_type | dsttype patterns |
|---|---|
| `tracking` | `DST_STREAMING_EVENT%`, `DST_TRKR_%` |
| `calo` | `DST_TRIGGERED_EVENT%`, `DST_CALOFITTING`, `DST_CALO`, `DST_JETS`, `DST_JETCALO` |

Source of truth for all current dsttypes: [sphenixjobdicts.py](sphenixjobdicts.py).

### Dashboard structure

One dashboard with a `$prod_type` dropdown variable (`tracking` / `calo`) rather than two separate dashboards. A second dashboard would mean every panel fix has to be applied twice.

---

## Dashboard layout

### Row 1 — Campaign health at a glance (stat panels)
- Job counts by status: `submitting`, `submitted`, `started`, `running`, `held`, `evicted`, `failed`, `finished` — color-coded tiles
- Overall completion % — `finished / maxjobsexpected` per tag
- Active jobs right now — `status IN ('started', 'running')`
- Failure rate (%) in a rolling time window

### Row 2 — Progress by rule/dataset (table panel)
The main "where are we?" view. One row per `(tag, dsttype, dataset)` with columns: expected, submitted, running, finished, failed. The `finished` cell scales green toward 100%; the `failed` cell scales red. Analogous to the Run Table in the sPHENIX Run Database dashboard.

### Row 3 — Performance and resource trends (time series)
- Jobs finishing per hour (throughput)
- `MemoryUsage` vs `MemoryProvisioned` — reveals how often memory-escalation retries are happening
- Average `RemoteUserCpu` per `dsttype`
- `jobstarts > 1` trend — jobs that restarted, proxy for cluster instability

---

## Dashboard variables (Grafana)

| variable | type | query |
|---|---|---|
| `$prod_type` | static dropdown | `tracking`, `calo` |
| `$tag` | dynamic query | `SELECT tag FROM production_overview WHERE active AND production_type = '${prod_type}' ORDER BY priority` |

Every panel filters on `tag = ANY($tag)` and uses a `dsttype`-based predicate to enforce the production_type boundary (so a calo query on tag X does not accidentally pull tracking jobs also carrying tag X).

---

## The `production_overview` table

### Purpose

Acts as the **control plane** for the dashboard. Curators flip `active = true` to make a campaign visible. New `(tag, production_type)` pairs are auto-registered by a Postgres trigger the moment the first matching job is inserted into `production_jobs`, so curators never have to remember to create entries manually.

### Schema

```sql
CREATE TABLE IF NOT EXISTS production_overview (
    id               SERIAL                   PRIMARY KEY,
    tag              TEXT                     NOT NULL,
    production_type  TEXT                     NOT NULL,
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
```

### Column notes

- **`active`** — defaults `false`; nothing appears on the dashboard until a curator explicitly enables it.
- **`display_name`** — optional human-friendly label for panel titles (e.g. `"Run 3 Au+Au pass-1"`).
- **`priority`** — controls display order; lower numbers appear first. Useful when multiple campaigns are active simultaneously.
- **`started_at` / `expected_end`** — curator-supplied timestamps for ETA and progress-bar panels.
- **`notes`** — free-text annotation surfaced as a dashboard tooltip. Intended for things like known issues, hold reasons, or links to logbooks.
- **`registered_at`** — set automatically on first job insert; gives curators context on when a campaign arrived without needing to query `production_jobs`.
- **`CHECK (production_type IN (...))`** — enforces the allowed values at the DB level independently of the trigger, so a direct INSERT by a curator cannot introduce an invalid type. Easy to extend when a third type appears.

### Auto-registration trigger

```sql
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
```

**`RAISE NOTICE` on unknown dsttype** — rather than silently skipping or aborting the job insert, an unknown dsttype emits a Postgres `NOTICE`. This is visible in server logs and to any client with `\set VERBOSITY verbose`. The intent is to catch new dsttypes early (e.g. `DST_TRKR_MVTXME` is already in `sphenixjobdicts.py` and would currently fall through) without ever blocking job submission.

**`CREATE OR REPLACE TRIGGER`** requires Postgres 14+. On older versions, replace with `DROP TRIGGER IF EXISTS trg_auto_register_overview ON production_jobs; CREATE TRIGGER ...`.

---

## Open questions / next steps

- **Panel queries** — write the SQL behind each Grafana panel before touching the dashboard JSON.
- **`DST_TRKR_MVTXME`** — currently falls through the trigger's `CASE` (not in the tracking pattern). Decide if it belongs to `tracking` and add it.
- **Unknown-dsttype alerting** — the `RAISE NOTICE` is passive. Consider a small separate table `production_unknown_dsttypes` that the trigger also inserts into, making it queryable and alertable from Grafana.
- **Curator tooling** — a minimal admin script or SQL snippet for the common curator operations: activate a campaign, set display name, set priority, add a note.
