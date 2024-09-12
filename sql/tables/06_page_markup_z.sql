-- Just like page_markup, but markup is ZStandard-compressed binary data
CREATE TABLE IF NOT EXISTS page_markup_z
(
    page_id INTEGER NOT NULL,
    markup  BLOB
);

