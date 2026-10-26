package core

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricName(t *testing.T) {
	tests := []struct {
		name     string
		labels   []string
		expected string
	}{
		{
			name:     "single label",
			labels:   []string{"task_name", "get_profit_summary"},
			expected: `dungbeetle_jobs_running{task_name="get_profit_summary"}`,
		},
		{
			name:     "multiple labels",
			labels:   []string{"task_name", "a", "queue", "default"},
			expected: `dungbeetle_jobs_running{task_name="a",queue="default"}`,
		},
		{
			name:     "quote",
			labels:   []string{"task_name", `report"daily`},
			expected: `dungbeetle_jobs_running{task_name="report\"daily"}`,
		},
		{
			name:     "backslash",
			labels:   []string{"task_name", `report\daily`},
			expected: `dungbeetle_jobs_running{task_name="report\\daily"}`,
		},
		{
			name:     "trailing backslash",
			labels:   []string{"task_name", `report\`},
			expected: `dungbeetle_jobs_running{task_name="report\\"}`,
		},
		{
			name:     "newline",
			labels:   []string{"task_name", "report\ndaily"},
			expected: `dungbeetle_jobs_running{task_name="report\ndaily"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := metricName(metricJobsRunning, tt.labels...)
			assert.Equal(t, tt.expected, got)
			assert.NoError(t, metrics.ValidateMetric(got))
		})
	}
}

type stubBroker struct {
	pending []string
	err     error
}

func (b stubBroker) Enqueue(context.Context, []byte, string) error { return nil }

func (b stubBroker) EnqueueScheduled(context.Context, []byte, string, time.Time) error { return nil }

func (b stubBroker) Consume(context.Context, chan []byte, string) {}

func (b stubBroker) GetPending(context.Context, string) ([]string, error) { return b.pending, b.err }

func TestRegisterQueueMetrics(t *testing.T) {
	tests := []struct {
		name     string
		broker   stubBroker
		expected string
	}{
		{
			name:     "pending jobs",
			broker:   stubBroker{pending: []string{"a", "b", "c"}},
			expected: `dungbeetle_queue_pending_jobs{queue="q"} 3`,
		},
		{
			name:     "broker error",
			broker:   stubBroker{err: errors.New("broker down")},
			expected: `dungbeetle_queue_pending_jobs{queue="q"} NaN`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			co := New(Opt{Broker: tt.broker}, nil, nil, slog.Default())
			co.registerQueueMetrics("q", 4)

			var buf bytes.Buffer
			co.Metrics().WritePrometheus(&buf)
			out := buf.String()

			require.Contains(t, out, tt.expected)
			assert.Contains(t, out, `dungbeetle_worker_concurrency{queue="q"} 4`)
		})
	}
}
