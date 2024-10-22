CREATE TABLE IF NOT EXISTS page
(
    id              INTEGER NOT NULL,
    namespace_id    INTEGER NOT NULL,
    page_type       INTEGER NOT NULL,
    last_edited     INTEGER NOT NULL,
    depth           INTEGER,
    title           TEXT NOT NULL,
    redirect_target TEXT
);

