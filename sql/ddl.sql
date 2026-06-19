-- Static dimension table: AI agent types.
-- Idempotent: safe to run more than once.

CREATE TABLE IF NOT EXISTS agent_types (
    id        INT PRIMARY KEY,
    type_name VARCHAR(100) NOT NULL
);
