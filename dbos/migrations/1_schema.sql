-- Consolidated DBOS schema. Single migration: this project is in rapid development
-- and only fresh databases are supported; historical incremental migrations were
-- collapsed into this end-state. Keep dbos/schema.sql (sqlc table shapes) in sync.
--
-- {schema} is replaced with the sanitized schema identifier at load time.

CREATE TABLE {schema}.workflow_status (
    workflow_uuid TEXT PRIMARY KEY,
    status TEXT,
    name TEXT,
    authenticated_user TEXT,
    assumed_role TEXT,
    authenticated_roles TEXT,
    request TEXT,
    output TEXT,
    error TEXT,
    error_encoded TEXT,
    executor_id TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint,
    application_version TEXT,
    application_id TEXT,
    class_name VARCHAR(255) DEFAULT NULL,
    config_name VARCHAR(255) DEFAULT NULL,
    recovery_attempts BIGINT DEFAULT 0,
    queue_name TEXT,
    workflow_timeout_ms BIGINT,
    workflow_deadline_epoch_ms BIGINT,
    inputs TEXT,
    started_at_epoch_ms BIGINT,
    deduplication_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    queue_partition_key TEXT,
    forked_from TEXT,
    owner_xid TEXT,
    parent_workflow_id TEXT,
    serialization TEXT,
    delay_until_epoch_ms BIGINT,
    was_forked_from BOOLEAN NOT NULL DEFAULT FALSE,
    rate_limited BOOLEAN NOT NULL DEFAULT FALSE,
    completed_at BIGINT,
    definition_digest TEXT
);

CREATE INDEX workflow_status_created_at_index ON {schema}.workflow_status (created_at);
CREATE INDEX idx_workflow_status_forked_from ON {schema}.workflow_status (forked_from) WHERE forked_from IS NOT NULL;
CREATE INDEX idx_workflow_status_parent_workflow_id ON {schema}.workflow_status (parent_workflow_id) WHERE parent_workflow_id IS NOT NULL;
CREATE UNIQUE INDEX uq_workflow_status_dedup_id ON {schema}.workflow_status (queue_name, deduplication_id) WHERE deduplication_id IS NOT NULL;
CREATE UNIQUE INDEX uq_workflow_status_name_dedup_id ON {schema}.workflow_status (name, deduplication_id) WHERE deduplication_id IS NOT NULL;
CREATE INDEX idx_workflow_status_pending ON {schema}.workflow_status (created_at) WHERE status = 'PENDING';
CREATE INDEX idx_workflow_status_failed ON {schema}.workflow_status (status, created_at) WHERE status IN ('ERROR', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED');
CREATE INDEX idx_workflow_status_in_flight ON {schema}.workflow_status (queue_name, status, priority, created_at) WHERE status IN ('ENQUEUED', 'PENDING');
CREATE INDEX idx_workflow_status_claim ON {schema}.workflow_status (name, status, priority, created_at) WHERE status IN ('ENQUEUED', 'PENDING');
CREATE INDEX idx_workflow_status_rate_limited ON {schema}.workflow_status (queue_name, started_at_epoch_ms) WHERE rate_limited = TRUE;
CREATE INDEX idx_workflow_status_completed_at ON {schema}.workflow_status (completed_at) WHERE completed_at IS NOT NULL;
CREATE INDEX idx_workflow_status_started_at ON {schema}.workflow_status (started_at_epoch_ms) WHERE started_at_epoch_ms IS NOT NULL;
CREATE INDEX idx_workflow_status_delayed ON {schema}.workflow_status (delay_until_epoch_ms) WHERE status = 'DELAYED';

CREATE TABLE {schema}.operation_outputs (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    function_name TEXT NOT NULL DEFAULT '',
    output TEXT,
    error TEXT,
    error_encoded TEXT,
    started_at_epoch_ms BIGINT,
    completed_at_epoch_ms BIGINT,
    serialization TEXT,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES {schema}.workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX idx_operation_outputs_completed_at_function_name ON {schema}.operation_outputs (completed_at_epoch_ms, function_name);

CREATE TABLE {schema}.notifications (
    destination_uuid TEXT NOT NULL,
    topic TEXT,
    message TEXT NOT NULL,
    created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint,
    message_uuid TEXT NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    serialization TEXT,
    consumed BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (destination_uuid) REFERENCES {schema}.workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX idx_notifications ON {schema}.notifications (destination_uuid, topic);

CREATE TABLE {schema}.workflow_events (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    serialization TEXT,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES {schema}.workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE {schema}.workflow_events_history (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    serialization TEXT,
    PRIMARY KEY (workflow_uuid, function_id, key),
    FOREIGN KEY (workflow_uuid) REFERENCES {schema}.workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE {schema}.streams (
    workflow_uuid TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    "offset" INTEGER NOT NULL,
    function_id INTEGER NOT NULL DEFAULT 0,
    serialization TEXT,
    PRIMARY KEY (workflow_uuid, key, "offset"),
    FOREIGN KEY (workflow_uuid) REFERENCES {schema}.workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE {schema}.event_dispatch_kv (
    service_name TEXT NOT NULL,
    workflow_fn_name TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    update_seq NUMERIC(38,0),
    update_time NUMERIC(38,15),
    PRIMARY KEY (service_name, workflow_fn_name, key)
);

CREATE TABLE {schema}.workflow_schedules (
    schedule_id TEXT PRIMARY KEY,
    schedule_name TEXT NOT NULL UNIQUE,
    workflow_name TEXT NOT NULL,
    workflow_class_name TEXT,
    schedule TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    context TEXT NOT NULL,
    last_fired_at TEXT DEFAULT NULL,
    automatic_backfill BOOLEAN NOT NULL DEFAULT FALSE,
    cron_timezone TEXT DEFAULT NULL,
    queue_name TEXT DEFAULT NULL
);

CREATE TABLE {schema}.application_versions (
    version_id TEXT NOT NULL PRIMARY KEY,
    version_name TEXT NOT NULL UNIQUE,
    version_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint
);

CREATE TABLE {schema}.queues (
    queue_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    name TEXT NOT NULL UNIQUE,
    concurrency INTEGER,
    worker_concurrency INTEGER,
    rate_limit_max INTEGER,
    rate_limit_period_sec DOUBLE PRECISION,
    priority_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    partition_queue BOOLEAN NOT NULL DEFAULT FALSE,
    polling_interval_sec DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
    updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);

-- Workflow definitions are immutable and content-addressed: one row per
-- (workflow_name, digest), where the digest covers the code-declared configuration.
-- The mutable workflow_current pointer selects the active definition per workflow
-- (moved by deploys); workflow_overrides holds operator-owned policy overrides that
-- survive deploys and are never written by application code.
CREATE TABLE {schema}.workflow_definitions (
    workflow_name TEXT NOT NULL,
    digest TEXT NOT NULL,
    input_schema TEXT,
    output_schema TEXT,
    debounce_delay_ms BIGINT,
    debounce_timeout_ms BIGINT,
    max_recovery_attempts BIGINT,
    global_concurrency INTEGER,
    rate_limit INTEGER,
    rate_period_ms BIGINT,
    workflow_retention_ms BIGINT NOT NULL DEFAULT 86400000 CHECK (workflow_retention_ms > 0),
    cron_schedule TEXT,
    created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint,
    PRIMARY KEY (workflow_name, digest)
);

CREATE TABLE {schema}.workflow_current (
    workflow_name TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    since BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now())::numeric * 1000)::bigint,
    FOREIGN KEY (workflow_name, digest) REFERENCES {schema}.workflow_definitions (workflow_name, digest)
);

CREATE TABLE {schema}.workflow_overrides (
    workflow_name TEXT PRIMARY KEY,
    global_concurrency INTEGER,
    rate_limit INTEGER,
    rate_period_ms BIGINT
);

-- Listen/notify plumbing for Send/Recv and SetEvent/GetEvent.
CREATE OR REPLACE FUNCTION {schema}.notifications_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.destination_uuid || '::' || NEW.topic;
BEGIN
    PERFORM pg_notify('dbos_notifications_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dbos_notifications_trigger
AFTER INSERT ON {schema}.notifications
FOR EACH ROW EXECUTE FUNCTION {schema}.notifications_function();

CREATE OR REPLACE FUNCTION {schema}.workflow_events_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_workflow_events_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dbos_workflow_events_trigger
AFTER INSERT ON {schema}.workflow_events
FOR EACH ROW EXECUTE FUNCTION {schema}.workflow_events_function();

-- plpgsql stored functions for direct SQL client access.
CREATE FUNCTION {schema}.enqueue_workflow(
    workflow_name TEXT,
    queue_name TEXT,
    positional_args JSON[] DEFAULT ARRAY[]::JSON[],
    named_args JSON DEFAULT '{}'::JSON,
    class_name TEXT DEFAULT NULL,
    config_name TEXT DEFAULT NULL,
    workflow_id TEXT DEFAULT NULL,
    app_version TEXT DEFAULT NULL,
    timeout_ms BIGINT DEFAULT NULL,
    deadline_epoch_ms BIGINT DEFAULT NULL,
    deduplication_id TEXT DEFAULT NULL,
    priority INTEGER DEFAULT NULL,
    queue_partition_key TEXT DEFAULT NULL
) RETURNS TEXT AS $$
DECLARE
    v_workflow_id TEXT;
    v_serialized_inputs TEXT;
    v_owner_xid TEXT;
    v_now BIGINT;
    v_recovery_attempts INTEGER := 0;
    v_priority INTEGER;
BEGIN

    -- Validate required parameters
    IF workflow_name IS NULL OR workflow_name = '' THEN
        RAISE EXCEPTION 'Workflow name cannot be null or empty';
    END IF;
    IF queue_name IS NULL OR queue_name = '' THEN
        RAISE EXCEPTION 'Queue name cannot be null or empty';
    END IF;
    IF named_args IS NOT NULL AND jsonb_typeof(named_args::jsonb) != 'object' THEN
        RAISE EXCEPTION 'Named args must be a JSON object';
    END IF;
    IF workflow_id IS NOT NULL AND workflow_id = '' THEN
        RAISE EXCEPTION 'Workflow ID cannot be an empty string if provided.';
    END IF;

    v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
    v_owner_xid := gen_random_uuid()::TEXT;
    v_priority := COALESCE(priority, 0);
    v_serialized_inputs := json_build_object(
        'positionalArgs', positional_args,
        'namedArgs', named_args
    )::TEXT;
    v_now := EXTRACT(epoch FROM now()) * 1000;

    INSERT INTO {schema}.workflow_status (
        workflow_uuid, status, inputs,
        name, class_name, config_name,
        authenticated_user, assumed_role,
        queue_name, deduplication_id, priority, queue_partition_key,
        application_version,
        created_at, updated_at, recovery_attempts,
        workflow_timeout_ms, workflow_deadline_epoch_ms,
        parent_workflow_id, owner_xid, serialization
    ) VALUES (
        v_workflow_id, 'ENQUEUED', v_serialized_inputs,
        workflow_name, class_name, config_name,
        '', '',
        queue_name, deduplication_id, v_priority, queue_partition_key,
        app_version,
        v_now, v_now, v_recovery_attempts,
        timeout_ms, deadline_epoch_ms,
        NULL, v_owner_xid, 'portable_json'
    )
    ON CONFLICT (workflow_uuid)
    DO UPDATE SET
        updated_at = EXCLUDED.updated_at;

    RETURN v_workflow_id;

EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'DBOS queue duplicated'
            USING DETAIL = format('Workflow %s with queue %s and deduplication ID %s already exists', v_workflow_id, queue_name, deduplication_id),
                ERRCODE = 'unique_violation';
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION {schema}.send_message(
    destination_id TEXT,
    message JSON,
    topic TEXT DEFAULT NULL,
    message_id TEXT DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    v_topic TEXT := COALESCE(topic, '__null__topic__');
    v_message_id TEXT := COALESCE(message_id, gen_random_uuid()::TEXT);
BEGIN
    INSERT INTO {schema}.notifications (
        destination_uuid, topic, message, message_uuid, serialization
    ) VALUES (
        destination_id, v_topic, message, v_message_id, 'portable_json'
    )
    ON CONFLICT (message_uuid) DO NOTHING;
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE EXCEPTION 'DBOS non-existent workflow'
            USING DETAIL = format('Destination workflow %s does not exist', destination_id),
                ERRCODE = 'foreign_key_violation';
END;
$$ LANGUAGE plpgsql;

-- Pin search_path so function references resolve only against the system catalog.
ALTER FUNCTION {schema}.enqueue_workflow(
    TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INTEGER, TEXT
) SET search_path = pg_catalog, pg_temp;

ALTER FUNCTION {schema}.send_message(
    TEXT, JSON, TEXT, TEXT
) SET search_path = pg_catalog, pg_temp;

ALTER FUNCTION {schema}.notifications_function() SET search_path = pg_catalog, pg_temp;
ALTER FUNCTION {schema}.workflow_events_function() SET search_path = pg_catalog, pg_temp;
