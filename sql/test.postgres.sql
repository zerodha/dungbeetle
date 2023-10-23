-- test.sql
-- concurrency parameter is associated with the queue.
-- Once a queue is set to a particular concurrency, it cannot be changed. 
-- In the below example both `get_profit_summary` and `get_profit_entries` use 
-- a common queue with concurrency = 5. It is okay to pass concurrency 
-- again in `get_profit_entries` as long as it is the same as the one defined initially (5)

-- name: get_profit_summary
-- db: my_db
-- concurrency: 5
SELECT SUM(amount) AS total, entry_date FROM entries WHERE user_id = $1 GROUP BY entry_date;

-- name: get_profit_entries
-- db: my_db
-- queue: summarise
SELECT * FROM entries WHERE user_id = $1;

-- name: get_profit_entries_by_date
SELECT * FROM entries WHERE user_id = $1 AND timestamp > $2 and timestamp < $3;

-- name: slow_query 
-- db: my_db
SELECT pg_sleep($1);
