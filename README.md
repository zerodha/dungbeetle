# SQL Jobber
A highly opinionated, distributed job-queue built specifically for defering and executing SQL query jobs (reads). 

- Standalone server that exposes HTTP APIs for managing jobs (list, post, status check, cancel jobs)
- Reads SQL queries from .sql files and registers them as tasks ready to be queued
- Supports MySQL and PostgreSQL
- Written in Go and built on top of [Machinery](https://github.com/RichardKnop/machinery). Supports multi-process, multi-threaded, asynchronous distributed job queueing via a common broker backend (Redis, AMQP etc.)
- Results from jobs are written into a Redis + [rediSQL](https://github.com/RedBeardLab/rediSQL) that can be further queried and transformed without affecting the main SQL database

**Requirements**: Redis 4.0+ with the [rediSQL](https://github.com/RedBeardLab/rediSQL) module


## Why?
SQL Jobber was built for certain specific use cases after solutions like Celery + Python for producing SQL reports were found to be resource intensive and too slow for our needs. We wanted to be able to distribute, multi-process, multi-thread, and separate SQL query logic from our core applications and prevent from our primary SQL databases from being inundated.

##### Usecase 1
Consider an application that has a very large SQL database. When there are several thousand concurrent users requesting reports from the database simultaneously, every second of IO delay in query execution locks up the application's threads, snowballing and overloading the application. Instead, we defer every single report request into a job queue, there by immediately freeing up the front end application. The reports are presented to users as they're executed (frontend polls the job's status and prevents the user from sending any more queries). Fixed SQL Jobber serviers and worker threads also act as traffic control and prevent our primary databases from being indundated with requests.

##### Usecase 2
Once the reports are generated, it's only natural for users to further transform the results by slicing, sorting and filtering, generating additional queries to the primary database. To offset this load, we send the results into an in-memory SQL database (Redis 4.0 + rediSQL, which is basically an in-memory, non-blocking, SQLite3 database running as a server inside Redis). Once the results of a particular query are available in this Redis instance, it's then possible to offer users fast transformations on their reports without further delays, with added SQLite 3 query goodness. These results are of course ephemeral and can be thrown away or expired.


![sql-job-server svg](https://user-images.githubusercontent.com/547147/34641268-6b8310ca-f327-11e7-95f2-bd2b6308586f.png)


## Concepts
#### Task
A task is a named SQL job is loaded into the server on startup. Tasks are defined in .sql files in the simple [goyesql](https://github.com/knadh/goyesql) format. Such queries are self-contained and produce the desired final output with neatly named columns. They can take arbitrary positional arguments for execution.

Example:
```sql
-- queries.sql

-- name: get_profit_summary
SELECT SUM(amount) AS total, entry_date FROM entries GROUP BY entry_date WHERE user_id = ?;

-- name: get_profit_entries
SELECT * FROM entries WHERE user_id = ?;

-- name: get_profit_entries_by_date
SELECT * FROM entries WHERE user_id = ? AND timestamp > ? and timestamp < ?;

-- name: get_profit_entries_by_date
-- raw: 1
-- This query will not be prepared (raw=1)
SELECT * FROM entries WHERE user_id = ? AND timestamp > ? and timestamp < ?;
```

Here, when the server starts, the queries `get_profit_summary` and `get_profit_entries` are registered automatically as tasks. Internally, the server validates and prepares these SQL statements (unless `raw: 1`). `?` are MySQL value placholders. For Postgres, the placeholders are `$1, $2 ...`

#### Job
A job is an instance of a named task that has been queued to run. Each job has an ID that can be used to track its status. If an ID is not passed explicitly, it is generated internally and returned. These IDs needn not be unique, but only one job with a certain ID can be running at any given point. For the next job with the same ID to be scheduled, the previous job has to finish execution. Using non-unique IDs like this is useful in cases where users can be prevented from sending multiple requests for the same reports, like in our usecases.

#### Result
The results from an SQL query job are written into a Redis + rediSQL instance from where they can be queried and accessed like a normal SQL database. This is configured in the configuration file.

The results are stored in a table (`results` by default) in a rediSQL "database" (appears as a key in Redis. Not to be confused with Redis' numerical databases) where the database name is the job ID with an optional prefix specified in the configuration file.

The schema of the `results` table is automatically generated from the results of the original SQL query and consists of native SQLite 3 data types. For more info, check out [rediSQL](http://redbeardlab.tech/rediSQL/).

Example:
```shell
$ redis-cli
127.0.0.1:6379> keys *
1) "sqldb_myjob"

127.0.0.1:6379> REDISQL.EXEC sqldb_myjob "SELECT * FROM results LIMIT 1;"
1) 1) (integer) 9999.99
   2) "2018-01-01 00:00:00"
   

# Note that the column 'total' is present in the results table based on the SELECT fields
# in the original 'get_profit_summary' query.

127.0.0.1:6379> REDISQL.EXEC sqldb_myjob "SELECT total FROM results LIMIT 1;"
1) 1) (integer) 9999.99

```

## Installation
#### 1) Install
`go get github.com/zerodhatech/sql-job-server`

This will install the binary `sql-jobber` to `$GOPATH/bin`. If you do not have Go installed, you can download a pre-compiled binary from the releases page.

### 2) Configure
Copy the `config.toml.sample` file as `config.toml` somewhere and edit the configuration values.

Make sure the Redis instance specified in the `result_backend` section of the configuration file has the [rediSQL](http://redbeardlab.tech/rediSQL/) module loaded.

### 3) Setup tasks
Write your SQL query tasks in `.sql` files in the `goyesql` format (as shown in the examples earlier) and put them in a directory somewhere.

### 4) Start the server
```shell
sql-jobber --config /path/to/config.toml --sql-directory /path/to/your/sql/queries

# Run 'sql-jobber --help' to see all supported arguments
```

Starting the server runs a set of workers listening on a default job queue. It also starts a web service on `http://127.0.0.1:6060` which is the control interface. It's possible to run the server without the HTTP interface by passing the `--worker-only` flag.


### Usage
All interactions are using simple HTTP requests.


| Method | URI                    |                                                 |
|--------|------------------------|-------------------------------------------------|
| GET    | /tasks                 | Returns the list of registered SQL tasks        |
| POST   | /tasks/{taskName}/jobs | Schedules a job for a given task                |
| GET    | /jobs/{jobID}          | Returns the status of a given job               |
| GET    | /jobs/queue/{queue}    | Returns the list of all pending jobs in a queue |
| DELETE | /jobs/{jobID}          | Deletes a pending job from the queue and immediately cancels its execution and frees the thread. Only the Go PostgreSQL driver cancels queries mid execution. MySQL server will keep continuing to execute the query. For MySQL, it's important to set `max_execution_time`  |


| Job param     |                                                                                                                                                   |   |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------|---|
| `job_id`      | (Optional) Alphanumeric ID for the job. Can be non-unique. If this is not passed, the server generates and returns one                            |   |
| `queue`     | (Optional) Queue to send the job to. Only workers listening on this queue will receive the jobs.                                                                                                              |   |
| `eta`         | (Optional) Timestamp (`yyyy-mm-dd hh:mm:ss`) at which the job should start. If this is not provided, the job is queued immediately.               |   |
| `retries`     | (Optional) The number of times a failed job should be retried. Default is 0                                                                      |   |
| `arg`         | (Optional) The positional argument to pass to the SQL query in the task being executed. This can be passed multiple times, one for each argument |   |

##### Schedule a job
```shell
$ curl localhost:6060/tasks/get_profit_entries_by_date/jobs -d "job_id=myjob" -d "arg=USER1" -d "arg=2017-12-01" -d "arg=2017-01-01"
{"status":"success","message":"","data":{"job_id":"myjob","task_name":"get_profit_entries_by_date","queue":"sqljob_queue","eta":null,"retries":0}}
```

##### Check the job's status
```shell
$ curl localhost:6060/jobs/myjob
{"status":"success","message":"","data":{"job_id":"myjob","status":"SUCCESS","results":[{"Type":"int64","Value":2}],"error":""}}~                                                                               

# `Results` shows the number of rows generated by the query.
```

## Advanced usage
### Multiple queues, workers, and job distribution
It's possible to run multiple workers on one or more machines that run different jobs with different concurrency levels independently of each other using different queues. Not all of these instances need to expose the HTTP service and can run as `--worker-only`. This doesn't really make a difference as long as all instances connect to the same broker backend. A job posted to any instance will be routed correctly to the right instances based on the `queue` parameter.

Often times, different queries have different priorities of execution. Some may need to return results faster than others. The below example shows two SQL Jobber servers being run, one with 30 workers and one with just 5 to process jobs of different priorities.


```shell
# Run the primary worker + HTTP control interface
sql-jobber --config /path/to/config.toml --sql-directory /path/to/sql/dir \
	--queue-name "high_priority" \
    --worker-name "high_priority_worker" \
    --worker-concurrency 30

# Run another worker on a different queue to handle low priority jobs
sql-jobber --config /path/to/config.toml --sql-directory /path/to/sql/dir \
	--queue-name "low_priority" \
    --worker-name "low_priority_worker" \
    --worker-concurrency 5 \
    --worker-only

# Send a job to the high priority queue.
$ curl localhost:6060/tasks/get_profit_entries_by_date/jobs -d "arg=USER1" -d "arg=2017-12-01" -d "arg=2017-01-01" \
	-d "queue=high_priority"

# Send another job to the low priority queue.
$ curl localhost:6060/tasks/get_profit_entries/jobs -d "arg=USER1" -d "queue=low_priority"
```

## License
Copyright (c) Zerodha Technology Pvt. Ltd. All rights reserved.
Licensed under the MIT License.
