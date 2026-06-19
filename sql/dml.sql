-- Seed the AI agent types dimension.
-- Idempotent: ON CONFLICT updates the name, so re-running creates no duplicates.

INSERT INTO agent_types (id, type_name) VALUES
    (1, 'RAG answer'),
    (2, 'Email assistant'),
    (3, 'Calendar planner'),
    (4, 'Resume helper'),
    (5, 'Code assistant'),
    (6, 'Telegram channel writer'),
    (7, 'Task prioritizer'),
    (8, 'Document summarizer')
ON CONFLICT (id) DO UPDATE SET type_name = EXCLUDED.type_name;
