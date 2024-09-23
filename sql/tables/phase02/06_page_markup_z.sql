-- Same contents as page_markup, but data is ZStandard-compressed binary data
CREATE TABLE IF NOT EXISTS page_markup_z
(
    page_id INTEGER NOT NULL,
    data  BLOB
);
