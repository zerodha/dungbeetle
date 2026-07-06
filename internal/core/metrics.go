package core

import "strings"

const metricPrefix = "dungbeetle_"

// Metric names.
const (
	metricJobsQueued  = "jobs_queued_total"
	metricJobsRunning = "jobs_running"
	metricJobsSuccess = "jobs_success_total"
	metricJobsFailed  = "jobs_failed_total"
)

// Label is a key-value pair for Prometheus metric labels.
type Label struct {
	Key, Value string
}

// MetricName builds a Prometheus metric name with optional labels.
// E.g. MetricName("jobs_failed_total", Label{"task_name", "my_query"}) -> `dungbeetle_jobs_failed_total{task_name="my_query"}`
func MetricName(base string, labels ...Label) string {
	if len(labels) == 0 {
		return metricPrefix + base
	}

	var b strings.Builder
	b.WriteString(metricPrefix)
	b.WriteString(base)
	b.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Key)
		b.WriteString(`="`)
		b.WriteString(l.Value)
		b.WriteByte('"')
	}
	b.WriteByte('}')

	return b.String()
}
