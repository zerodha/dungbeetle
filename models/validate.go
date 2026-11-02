package models

import (
	"errors"
	"fmt"
	"regexp"
)

// reValidateJobID represents the character classes allowed in a job ID.
// A job ID becomes part of the name of the results table that the job writes
// to, so it is restricted to characters that are safe and portable inside an
// SQL identifier.
var reValidateJobID = regexp.MustCompile("(?i)^[a-z0-9-_:]+$")

// MaxJobIDLen is the maximum length of a job ID. Postgres truncates identifiers
// at 63 bytes and MySQL rejects them past 64, so two long job IDs sharing a
// prefix would otherwise map to the same results table. 55 leaves room for the
// default `results_%s` table name template.
const MaxJobIDLen = 55

// ValidateJobID validates a caller-supplied job ID. An empty ID is valid, as
// the server generates one in that case.
func ValidateJobID(id string) error {
	if id == "" {
		return nil
	}

	if len(id) > MaxJobIDLen {
		return fmt.Errorf("`job_id` cannot be longer than %d characters", MaxJobIDLen)
	}

	if !reValidateJobID.MatchString(id) {
		return errors.New("invalid characters in the `job_id`")
	}

	return nil
}
