CREATE TABLE IF NOT EXISTS sense_training_context_page (
    context_id INTEGER NOT NULL,
    page_id INTEGER NOT NULL,
    weight REAL NOT NULL,
    PRIMARY KEY (context_id, page_id),
    FOREIGN KEY (context_id) REFERENCES sense_training_context(context_id),
    FOREIGN KEY (page_id) REFERENCES page(id)
);