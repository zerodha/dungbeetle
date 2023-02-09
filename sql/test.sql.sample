-- test.sql

-- name: get_profit_summary
-- db: my_db
SELECT SUM(amount) AS total, entry_date FROM entries WHERE user_id = $1 GROUP BY entry_date;

-- name: get_profit_entries
-- db: my_db
SELECT * FROM entries WHERE user_id = $1;

-- name: get_profit_entries_by_date
SELECT * FROM entries WHERE user_id = $1 AND timestamp > $2 and timestamp < $3;
