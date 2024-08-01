create schema eventlogs;

create table eventlogs.events (
    log_id varchar(128) not null,
    event_index OID not null,
    recorded_at timestamp with time zone not null,
    idempotency_key varchar(256) null constraint idempotency_key_unique unique,
    payload json not null,
    primary key(log_id, event_index)
);
