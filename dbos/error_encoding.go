package dbos

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"

	cockroacherrors "github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errbase"
	"github.com/cockroachdb/errors/errorspb"
	"github.com/gogo/protobuf/proto"
)

// Workflow and step errors round-trip through the database. The `error` column
// stores the human-readable message for auditing and cross-language tooling;
// `error_encoded` stores a cockroachdb/errors representation (base64 protobuf)
// so Go callers recover typed errors (errors.Is / errors.As) when results are
// read back from the database.

// dbosErrorFields is the wire representation of DBOSError's structured fields.
type dbosErrorFields struct {
	Message         string        `json:"message"`
	Code            DBOSErrorCode `json:"code"`
	WorkflowID      string        `json:"workflow_id,omitempty"`
	DestinationID   string        `json:"destination_id,omitempty"`
	StepName        string        `json:"step_name,omitempty"`
	QueueName       string        `json:"queue_name,omitempty"`
	DeduplicationID string        `json:"deduplication_id,omitempty"`
	StepID          int           `json:"step_id,omitempty"`
	ExpectedName    string        `json:"expected_name,omitempty"`
	RecordedName    string        `json:"recorded_name,omitempty"`
	MaxRetries      int           `json:"max_retries,omitempty"`
}

func encodeDBOSErrorFields(err *DBOSError) []string {
	fields := dbosErrorFields{
		Message:         err.Message,
		Code:            err.Code,
		WorkflowID:      err.WorkflowID,
		DestinationID:   err.DestinationID,
		StepName:        err.StepName,
		QueueName:       err.QueueName,
		DeduplicationID: err.DeduplicationID,
		StepID:          err.StepID,
		ExpectedName:    err.ExpectedName,
		RecordedName:    err.RecordedName,
		MaxRetries:      err.MaxRetries,
	}
	b, jsonErr := json.Marshal(fields)
	if jsonErr != nil {
		return nil
	}
	return []string{string(b)}
}

func decodeDBOSErrorFields(safeDetails []string) *DBOSError {
	if len(safeDetails) == 0 {
		return nil
	}
	var fields dbosErrorFields
	if err := json.Unmarshal([]byte(safeDetails[0]), &fields); err != nil {
		return nil
	}
	return &DBOSError{
		Message:         fields.Message,
		Code:            fields.Code,
		WorkflowID:      fields.WorkflowID,
		DestinationID:   fields.DestinationID,
		StepName:        fields.StepName,
		QueueName:       fields.QueueName,
		DeduplicationID: fields.DeduplicationID,
		StepID:          fields.StepID,
		ExpectedName:    fields.ExpectedName,
		RecordedName:    fields.RecordedName,
		MaxRetries:      fields.MaxRetries,
	}
}

func init() {
	// DBOSError is a leaf when it wraps nothing and a wrapper otherwise, so
	// register codecs for both shapes under its type key.
	typeKey := errbase.GetTypeKey(&DBOSError{})

	errbase.RegisterLeafEncoder(typeKey, func(_ context.Context, err error) (string, []string, proto.Message) {
		dbosErr := err.(*DBOSError)
		return dbosErr.Error(), encodeDBOSErrorFields(dbosErr), nil
	})
	errbase.RegisterLeafDecoder(typeKey, func(_ context.Context, _ string, safeDetails []string, _ proto.Message) error {
		if decoded := decodeDBOSErrorFields(safeDetails); decoded != nil {
			return decoded
		}
		return nil // fall back to the library's opaque representation
	})

	errbase.RegisterWrapperEncoder(typeKey, func(_ context.Context, err error) (string, []string, proto.Message) {
		dbosErr := err.(*DBOSError)
		return dbosErr.Error(), encodeDBOSErrorFields(dbosErr), nil
	})
	errbase.RegisterWrapperDecoder(typeKey, func(_ context.Context, cause error, _ string, safeDetails []string, _ proto.Message) error {
		if decoded := decodeDBOSErrorFields(safeDetails); decoded != nil {
			decoded.wrappedErr = cause
			return decoded
		}
		return nil // fall back to the library's opaque representation
	})
}

// encodeWorkflowError encodes an error into a base64 protobuf string for the
// error_encoded column. Returns nil if the error cannot be encoded.
func encodeWorkflowError(err error) *string {
	if err == nil {
		return nil
	}
	encoded := cockroacherrors.EncodeError(context.Background(), err)
	b, marshalErr := proto.Marshal(&encoded)
	if marshalErr != nil {
		return nil
	}
	s := base64.StdEncoding.EncodeToString(b)
	return &s
}

// decodeWorkflowError decodes an error from the error_encoded column. Returns
// nil if the input is absent or cannot be decoded; callers should then fall
// back to the human-readable error column.
func decodeWorkflowError(encoded *string) error {
	if encoded == nil || *encoded == "" {
		return nil
	}
	b, decodeErr := base64.StdEncoding.DecodeString(*encoded)
	if decodeErr != nil {
		return nil
	}
	var enc errorspb.EncodedError
	if unmarshalErr := proto.Unmarshal(b, &enc); unmarshalErr != nil {
		return nil
	}
	decoded := cockroacherrors.DecodeError(context.Background(), enc)

	// context.Canceled is a plain errors.New sentinel: it decodes to an opaque
	// leaf that only cockroachdb's marker-based Is recognizes. Re-anchor it so
	// the standard library's errors.Is works too.
	if !errors.Is(decoded, context.Canceled) && cockroacherrors.Is(decoded, context.Canceled) {
		return &sentinelError{msg: decoded.Error(), sentinel: context.Canceled}
	}
	return decoded
}

// sentinelError preserves a decoded error's message while anchoring it to a
// standard library sentinel for errors.Is.
type sentinelError struct {
	msg      string
	sentinel error
}

func (e *sentinelError) Error() string { return e.msg }
func (e *sentinelError) Unwrap() error { return e.sentinel }
