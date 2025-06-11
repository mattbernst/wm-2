CREATE TABLE IF NOT EXISTS training_example (
    example_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_page_id INTEGER NOT NULL,  -- References the page from SenseFeatures that contains this example
    context_id INTEGER NOT NULL,     -- References the context for this example
    source_page_id INTEGER NOT NULL,
    link_destination INTEGER NOT NULL,
    label TEXT NOT NULL,
    sense_id INTEGER NOT NULL,
    commonness REAL NOT NULL,
    relatedness REAL NOT NULL,
    context_quality REAL NOT NULL,
    is_correct_sense BOOLEAN NOT NULL,
    FOREIGN KEY (sense_page_id) REFERENCES page(id),
    FOREIGN KEY (context_id) REFERENCES training_context(context_id),
    FOREIGN KEY (source_page_id) REFERENCES page(id),
    FOREIGN KEY (link_destination) REFERENCES page(id)
);
