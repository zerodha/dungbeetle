package core

import (
	"context"
	"math"
	"runtime"
	"strings"
)

// Metric names, without the dbtl_ prefix.
const (
	metricJobsEnqueued     = "jobs_enqueued_total"
	metricJobsRunning      = "jobs_running"
	metricJobAttempts      = "job_attempts_total"
	metricJobExecDuration  = "job_execution_duration_seconds"
	metricJobQueueWait     = "job_queue_wait_duration_seconds"
	metricQueuePendingJobs = "queue_pending_jobs"
	metricWorkerConc       = "worker_concurrency"
)

// durationBuckets are histogram upper bounds in seconds, sized for SQL jobs
// that can take anywhere from milliseconds to an hour.
var durationBuckets = []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600}

// labelEscaper escapes label values as required by the Prometheus text format.
var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

// metricName returns the prefixed metric name with labels given as key, value pairs.
// E.g. metricName("jobs_running", "task_name", "a") -> `dbtl_jobs_running{task_name="a"}`
func metricName(name string, labels ...string) string {
	var b strings.Builder
	b.WriteString("dbtl_")
	b.WriteString(name)
	b.WriteByte('{')
	for i := 0; i+1 < len(labels); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(labels[i])
		b.WriteString(`="`)
		labelEscaper.WriteString(&b, labels[i+1])
		b.WriteByte('"')
	}
	b.WriteByte('}')

	return b.String()
}

// registerQueueMetrics registers the pending jobs and worker concurrency gauges for a queue.
// Repeat calls for the same queue are no-ops.
func (co *Core) registerQueueMetrics(queue string, conc int) {
	// Mirror tasqueue's default concurrency for tasks that don't set one.
	if conc <= 0 {
		conc = runtime.GOMAXPROCS(0)
	}
	co.metrics.GetOrCreateGauge(metricName(metricWorkerConc, "queue", queue), func() float64 {
		return float64(conc)
	})

	// Evaluated on every scrape. The broker only counts runnable jobs, so ETA-scheduled ones aren't counted.
	co.metrics.GetOrCreateGauge(metricName(metricQueuePendingJobs, "queue", queue), func() float64 {
		n, err := co.opt.Broker.GetPendingCount(context.Background(), queue)
		if err != nil {
			co.lo.Error("error fetching pending jobs for metrics", "queue", queue, "error", err)
			return math.NaN()
		}

		return float64(n)
	})
}
