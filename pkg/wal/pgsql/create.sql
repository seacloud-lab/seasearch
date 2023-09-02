CREATE DATABASE zincsearch;

\c zincsearch;

CREATE TABLE IF NOT EXISTS "wal"
(
    "id"        bigserial PRIMARY KEY NOT NULL,
    "index"     bigint NOT NULL,
    "name"      varchar(100) NOT NULL,
    "data"      text NOT NULL
    );

CREATE INDEX IF NOT EXISTS "wal_index"
    ON "wal"("name", "index");

CREATE TABLE IF NOT EXISTS "redo"
(
    "id"        bigserial PRIMARY KEY NOT NULL,
    "index"     int NOT NULL,
    "name"      varchar(100) NOT NULL,
    "data"      text NOT NULL
    );

CREATE INDEX IF NOT EXISTS "redo_index"
    ON "redo"("name");