package core

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricName(t *testing.T) {
	assert.Equal(t, "dungbeetle_jobs_queued_total", MetricName(metricJobsQueued))

	assert.Equal(t, `dungbeetle_jobs_failed_total{task_name="get_profit_summary"}`,
		MetricName(metricJobsFailed, Label{"task_name", "get_profit_summary"}))

	assert.Equal(t, `dungbeetle_jobs_running{task_name="a",queue="default"}`,
		MetricName(metricJobsRunning, Label{"task_name", "a"}, Label{"queue", "default"}))
}

func TestJobMetricsQueued(t *testing.T) {
	co := New(Opt{}, nil, nil, slog.Default())

	co.metrics.GetOrCreateCounter(MetricName(metricJobsQueued, Label{"task_name", "task_a"})).Inc()
	co.metrics.GetOrCreateCounter(MetricName(metricJobsQueued, Label{"task_name", "task_a"})).Inc()
	co.metrics.GetOrCreateCounter(MetricName(metricJobsQueued, Label{"task_name", "task_b"})).Inc()

	assert.EqualValues(t, 2, co.metrics.GetOrCreateCounter(MetricName(metricJobsQueued, Label{"task_name", "task_a"})).Get())
	assert.EqualValues(t, 1, co.metrics.GetOrCreateCounter(MetricName(metricJobsQueued, Label{"task_name", "task_b"})).Get())
}

func TestJobMetricsRunningGauge(t *testing.T) {
	co := New(Opt{}, nil, nil, slog.Default())

	running := co.metrics.GetOrCreateGauge(MetricName(metricJobsRunning, Label{"task_name", "task_a"}), nil)

	// Simulate two concurrent executions of the same task starting...
	running.Inc()
	running.Inc()
	assert.EqualValues(t, 2, running.Get())

	// ...and one finishing.
	running.Dec()
	assert.EqualValues(t, 1, running.Get())

	running.Dec()
	assert.EqualValues(t, 0, running.Get())
}

func TestJobMetricsSuccessAndFailed(t *testing.T) {
	co := New(Opt{}, nil, nil, slog.Default())

	success := co.metrics.GetOrCreateCounter(MetricName(metricJobsSuccess, Label{"task_name", "task_a"}))
	failed := co.metrics.GetOrCreateCounter(MetricName(metricJobsFailed, Label{"task_name", "task_a"}))

	success.Inc()
	failed.Inc()
	failed.Inc()

	assert.EqualValues(t, 1, success.Get())
	assert.EqualValues(t, 2, failed.Get())
}

func TestMetricsWritePrometheus(t *testing.T) {
	co := New(Opt{}, nil, nil, slog.Default())

	co.metrics.GetOrCreateCounter(MetricName(metricJobsQueued, Label{"task_name", "task_a"})).Inc()
	co.metrics.GetOrCreateGauge(MetricName(metricJobsRunning, Label{"task_name", "task_a"}), nil).Inc()

	var buf bytes.Buffer
	co.Metrics().WritePrometheus(&buf)
	out := buf.String()

	assert.Contains(t, out, `dungbeetle_jobs_queued_total{task_name="task_a"} 1`)
	assert.Contains(t, out, `dungbeetle_jobs_running{task_name="task_a"} 1`)
}
