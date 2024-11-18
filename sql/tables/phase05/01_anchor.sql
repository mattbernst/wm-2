CREATE TABLE IF NOT EXISTS anchor
(
    label                TEXT NOT NULL PRIMARY KEY,
    occurrence_count     INTEGER NOT NULL,
    occurrence_doc_count INTEGER NOT NULL,
    link_count           INTEGER NOT NULL,
    link_doc_count       INTEGER NOT NULL
);
