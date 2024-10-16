-- test.sql
-- concurrency parameter is associated with the queue.
-- Once a queue is set to a particular concurrency, it cannot be changed. 
-- In the below example both `get_profit_summary` and `get_profit_entries` use 
-- a common queue with concurrency = 5. It is okay to pass concurrency 
-- again in `get_profit_entries` as long as it is the same as the one defined initially (5)

-- name: get_profit_summary
-- db: my_db
-- concurrency: 5
-- queue: test
SELECT SUM(amount) AS total, entry_date FROM entries WHERE user_id = ? GROUP BY entry_date;

-- name: get_profit_entries
-- db: my_db
-- queue: test
SELECT * FROM entries WHERE user_id = ?;

-- name: get_profit_entries_by_date
-- queue: test
SELECT * FROM entries WHERE user_id = ? AND timestamp > ? and timestamp < ?;

-- name: slow_query 
-- db: my_db
-- queue: test
SELECT SLEEP(?);