-- Same contents as markup, but data is ZStandard-compressed binary data
CREATE TABLE IF NOT EXISTS markup_z
(
    page_id INTEGER PRIMARY KEY,
    data  BLOB
);
