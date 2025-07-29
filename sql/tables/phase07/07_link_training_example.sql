CREATE TABLE IF NOT EXISTS link_training_example (
    example_id INTEGER PRIMARY KEY AUTOINCREMENT,
    context_id INTEGER NOT NULL,     -- References the context for this example
    group_name TEXT NOT NULL,        -- Training group (e.g., "training", "test", "validation")
    source_page_id INTEGER NOT NULL, -- References the page from LinkFeatures that contains this example
    sense_page_title TEXT NOT NULL,
    sense_id INTEGER NOT NULL,
    normalized_occurrences REAL NOT NULL,
    max_disambig_confidence REAL NOT NULL,
    avg_disambig_confidence REAL NOT NULL,
    relatedness_to_context REAL NOT NULL,
    relatedness_to_other_topics REAL NOT NULL,
    avg_link_probability REAL NOT NULL,
    max_link_probability REAL NOT NULL,
    first_occurrence REAL NOT NULL,
    last_occurrence REAL NOT NULL,
    spread REAL NOT NULL,
    is_valid_link BOOLEAN NOT NULL,
    FOREIGN KEY (sense_id) REFERENCES page(id),
    FOREIGN KEY (context_id) REFERENCES sense_training_context(context_id)
);
