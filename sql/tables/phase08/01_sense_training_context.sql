CREATE TABLE IF NOT EXISTS sense_training_context (
    context_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_page_id INTEGER NOT NULL,  -- The page from SenseFeatures
    quality REAL NOT NULL,
    FOREIGN KEY (sense_page_id) REFERENCES page(id)
);
