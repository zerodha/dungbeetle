# SQL Jobber
> This is a highly experimental alpha version. Use with care.

A highly opinionated, distributed job-queue built specifically for defering and executing SQL read query jobs. 

- Standalone server that exposes HTTP APIs for managing jobs and groups of jobs (list, post, status check, cancel jobs)
- Reads SQL queries from .sql files and registers them as jobs ready to be queued
- Supports MySQL and PostgreSQL as data sources for tasks
- Supports MySQL and PostgreSQL as result stores for task responses
- Written in Go and built on top of [Machinery](https://github.com/RichardKnop/machinery). Supports multi-process, multi-threaded, asynchronous distributed job queueing via a common broker backend (Redis, AMQP etc.)

## Why?

##### Usecase 1
Consider an application that has a very large SQL database. When there are several thousand concurrent users requesting reports from the database simultaneously, every second of IO delay in query execution locks up the application's threads, snowballing and overloading the application. Instead, we defer every single report request into a job queue, there by immediately freeing up the front end application. The reports are presented to users as they're executed (frontend polls the job's status and prevents the user from sending any more queries). Fixed SQL Jobber servers and worker threads also act as traffic control and prevent the primary database from being indundated with requests.

##### Usecase 2
Once the reports are generated, it's only natural for users to further transform the results by slicing, sorting and filtering, generating additional queries to the primary database. To offset this load, these subsequent queries can be sent to a smaller, much faster database where the results of the original read query jobs are stored. These results are of course ephemeral and can be thrown away or expired.

![sql-job-server png](https://user-images.githubusercontent.com/547147/42087713-a1ac9f20-7bb4-11e8-9ce1-08b01afc26ae.png)

## Concepts
#### Task
A task is a named SQL job is loaded into the server on startup. Tasks are defined in .sql files in the simple [goyesql](https://github.com/knadh/goyesql) format. Such queries are self-contained and produce the desired final output with neatly named columns. They can take arbitrary positional arguments for execution. A task can be attached to one or more specific databases defined in the configuration using the `-- db:` tag. In case of multiple databases, the query will be executed against a random one from the list. A `-- queue:` tag to always route the task to a particular queue, unless it's overriden by the `queue` param when making a job request. A `-- results:` tag specifies the results backend to which the results of a task will be written. If there are multiple result backends specified, the results are written a random one.

Example:
```sql
-- queries.sql

-- name: get_profit_summary
SELECT SUM(amount) AS total, entry_date FROM entries GROUP BY entry_date WHERE user_id = ?;

-- name: get_profit_entries
-- db: db0, other_db
-- queue: myqueue
-- results: my_res_db
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

#### Results
The results from an SQL query job are written to a result backend (MySQL or Postgres) from where they can be further read or queried. This is configured in the configuration file. The results from a job are written to a new table named after that job, where schema of the results table is automatically generated from the results of the original SQL query. All fields are transformed into one of these types `BIGINT, DECIMAL, TIMESTAMP, DATE, BOOLEAN, TEXT`.


## Installation
### 1) Install
A pre-compiled binary can be downloaded from the [releases](releases) page.

or:

`go get github.com/zerodhatech/sql-job-server` to install the binary `sql-jobber` in `$GOPATH/bin`.

### 2) Configure
Copy the `config.toml.sample` file as `config.toml` somewhere and edit the configuration values.

### 3) Setup tasks
Write your SQL query tasks in `.sql` files in the `goyesql` format (as shown in the examples earlier) and put them in a directory somewhere.

### 4) Start the server
```shell
sql-jobber --config /path/to/config.toml --sql-directory /path/to/your/sql/queries

# Run 'sql-jobber --help' to see all supported arguments
```

Starting the server runs a set of workers listening on a default job queue. It also starts an HTTP service on `http://127.0.0.1:6060` which is the control interface. It's possible to run the server without the HTTP interface by passing the `--worker-only` flag.


### Usage
| Method | URI                    |                                                 |
|--------|------------------------|-------------------------------------------------|
| GET    | /tasks                 | Returns the list of registered SQL tasks        |
| POST   | /tasks/{taskName}/jobs | Schedules a job for a given task                |
| GET    | /jobs/{jobID}          | Returns the status of a given job               |
| GET    | /jobs/queue/{queue}    | Returns the list of all pending jobs in a queue |
| POST   | /groups                | Schedule a group of jobs                        |
| GET    | /groups/{groupID}      | Get the status of a job group and its jobs      |
| DELETE | /jobs/{jobID}          | Deletes a pending job from the queue and immediately cancels its execution and frees the thread. Only the Go PostgreSQL driver cancels queries mid execution. MySQL server will keep continuing to execute the query. For MySQL, it's important to set `max_execution_time`  |

POST requests accept raw JSON bodies. The JSON params are listed below.

| Job param     |                                                                                                                                                   |   |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------|---|
| `job_id` *string*      | (Optional) Alphanumeric ID for the job. Can be non-unique. If this is not passed, the server generates and returns one                            |   |
| `queue` *string*     | (Optional) Queue to send the job to. Only workers listening on this queue will receive the jobs.                                                                                                              |   |
| `eta` *string*         | (Optional) Timestamp (`yyyy-mm-dd hh:mm:ss`) at which the job should start. If this is not provided, the job is queued immediately.               |   |
| `retries` *int*     | (Optional) The number of times a failed job should be retried. Default is 0                                                                      |   |
| `ttl` *int*     | (Optional) TTL for the results in the results backend for this particular job                                                                      |   |
| `args[]` *[]string*         | (Optional) The positional argument to pass to the SQL query in the task being executed. This can be passed multiple times, one for each argument |   |


| Group param     |                                                                                                                                                   |   |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------|---|
| `group_id` *string*      | (Optional) Alphanumeric ID for the group of jobs. Can be non-unique. If this is not passed, the server generates and returns one                            |   |
| `concurrency` *int*     | (Optional) Number of jobs to run concurrently in the group                                                         

##### Schedule a job
```shell
$ curl localhost:6060/tasks/get_profit_entries_by_date/jobs -H "Content-Type: application/json" -X POST --data '{"job_id": "myjob", "args": ["USER1", "2017-12-01", "2017-01-01"]}'

{"status":"success","data":{"job_id":"myjob","task_name":"get_profit_entries_by_date","queue":"sqljob_queue","eta":null,"retries":0}}
```

##### Schedule a group of jobs
Sometimes, it's necessary to schedule a group of jobs and perform an action once they're all complete. Group jobs here run concurrently and independent of each other. The group state can be polled to figure out if all the jobs in it have finished executing.

```shell
$ curl localhost:6060/groups -H "Content-Type: application/json" -X POST --data '{"group_id": "mygroup", "concurrency": 3, "jobs": [{"job_id": "myjob", "task": "get_profit_entries_by_date", "args": ["USER1", "2017-12-01", "2017-01-01"]}, {"job_id": "myjob2", "task": "get_profit_entries_by_date", "args": ["USER1", "2017-12-01", "2017-01-01"]}]'

{"status":"success","data":{"group_id":"mygroup","jobs":[{"job_id":"myjob","task":"test1","queue":"sqljob_queue","eta":null,"retries":0},{"job_id":"myjob2","task":"test2","queue":"sqljob_queue","eta":null,"retries":0}]}}
```


##### Check a job's status
```shell
$ curl localhost:6060/jobs/myjob
{"status":"success","data":{"job_id":"myjob","status":"SUCCESS","results":[{"Type":"int64","Value":2}],"error":""}}~                                                                               

# `Results` indicates the number of rows generated by the query.
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
$ curl localhost:6060/tasks/get_profit_entries_by_date/jobs -H "Content-Type: application/json" --data '{"job_id": "myjob", "queue": "high_priority", "args": ["USER1", "2017-12-01", "2017-01-01"]}'

# Send another job to the low priority queue.
$ curl localhost:6060/tasks/get_profit_entries_by_date/jobs -H "Content-Type: application/json" --data '{"job_id": "myjob", "queue": "low_priority"}'
```

## License
Licensed under the MIT License.
