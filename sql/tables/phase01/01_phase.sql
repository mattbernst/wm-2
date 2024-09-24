CREATE TABLE IF NOT EXISTS phase
(
    id              INTEGER NOT NULL PRIMARY KEY,
    description     TEXT NOT NULL,
    start_ts        INTEGER NOT NULL,
    end_ts          INTEGER,
    state           TEXT NOT NULL
);

