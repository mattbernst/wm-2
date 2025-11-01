CREATE TABLE IF NOT EXISTS link_training_context (
    context_id INTEGER PRIMARY KEY AUTOINCREMENT,
    link_page_id INTEGER NOT NULL,  -- The page from LinkFeatures
    group_name TEXT NOT NULL,        -- Training group (e.g., "training", "test", "validation")
    quality REAL NOT NULL,
    FOREIGN KEY (link_page_id) REFERENCES page(id)
);
