-- A persisted explicit retry remains a proposal requiring fresh owned validation.
CREATE TABLE reconciliation_retry_proposals (
    run_id TEXT NOT NULL,
    page_id TEXT NOT NULL,
    proposal TEXT NOT NULL,
    PRIMARY KEY(run_id, page_id),
    FOREIGN KEY(run_id, page_id) REFERENCES reconciliation_plan_progress(run_id, page_id)
);
