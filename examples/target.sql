-- Create explicitly before registering examples/mysql-postgres.json.
CREATE TABLE public.research_items (
    id bigint PRIMARY KEY,
    body bytea,
    label text,
    created_at text,
    amount text
);
