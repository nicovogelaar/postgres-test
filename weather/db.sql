CREATE TABLE locations (
    device_id   varchar not null,
    location    varchar not null,
    environment varchar not null
);

CREATE TABLE conditions (
    time        timestamp with time zone not null,
    device_id   varchar not null,
    temperature double precision not null,
    humidity    double precision not null
);

CREATE INDEX conditions_device_id_time_idx ON conditions (device_id, "time" DESC);
CREATE INDEX conditions_time_idx ON conditions ("time" DESC);
