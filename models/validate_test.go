package models

import (
	"strings"
	"testing"
)

func TestValidateJobID(t *testing.T) {
	for _, c := range []struct {
		name string
		id   string
		ok   bool
	}{
		{"empty is generated server-side", "", true},
		{"alphanumeric", "myjob123", true},
		{"dash underscore colon", "my-job_1:a", true},
		{"uuid form", "job_2f1a7c6e-3b9d-4f2a-8c11-5d6e7f809a1b", true},
		{"at max length", strings.Repeat("a", MaxJobIDLen), true},

		{"over max length", strings.Repeat("a", MaxJobIDLen+1), false},
		{"double quote breaks postgres identifier", `a", "victim`, false},
		{"backtick breaks mysql identifier", "a`, `victim", false},
		{"stacked statement", `s"; DROP TABLE victim; COMMIT; --`, false},
		{"semicolon", "a;b", false},
		{"space", "a b", false},
		{"newline", "abc\n", false},
		{"null byte", "abc\x00", false},
		{"dot", "results.victim", false},
		{"percent breaks the table name template", "a%sb", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateJobID(c.id)
			if c.ok && err != nil {
				t.Fatalf("ValidateJobID(%q) = %v, want nil", c.id, err)
			}
			if !c.ok && err == nil {
				t.Fatalf("ValidateJobID(%q) = nil, want error", c.id)
			}
		})
	}
}
