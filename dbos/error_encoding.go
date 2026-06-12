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

type dbosErrorFields struct {
	Message         string        `json:"message"`
	Code            DbosErrorCode `json:"code"`
	WorkflowId      string        `json:"workflow_id,omitempty"`
	DestinationId   string        `json:"destination_id,omitempty"`
	StepName        string        `json:"step_name,omitempty"`
	QueueName       string        `json:"queue_name,omitempty"`
	DeduplicationId string        `json:"deduplication_id,omitempty"`
	StepId          int           `json:"step_id,omitempty"`
	ExpectedName    string        `json:"expected_name,omitempty"`
	RecordedName    string        `json:"recorded_name,omitempty"`
	MaxRetries      int           `json:"max_retries,omitempty"`
}

func encodeDbosErrorFields(err *DbosError) []string {
	fields := dbosErrorFields{
		Message:         err.Message,
		Code:            err.Code,
		WorkflowId:      err.WorkflowId,
		DestinationId:   err.DestinationId,
		StepName:        err.StepName,
		QueueName:       err.QueueName,
		DeduplicationId: err.DeduplicationId,
		StepId:          err.StepId,
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

func decodeDbosErrorFields(safeDetails []string) *DbosError {
	if len(safeDetails) == 0 {
		return nil
	}
	var fields dbosErrorFields
	if err := json.Unmarshal([]byte(safeDetails[0]), &fields); err != nil {
		return nil
	}
	return &DbosError{
		Message:         fields.Message,
		Code:            fields.Code,
		WorkflowId:      fields.WorkflowId,
		DestinationId:   fields.DestinationId,
		StepName:        fields.StepName,
		QueueName:       fields.QueueName,
		DeduplicationId: fields.DeduplicationId,
		StepId:          fields.StepId,
		ExpectedName:    fields.ExpectedName,
		RecordedName:    fields.RecordedName,
		MaxRetries:      fields.MaxRetries,
	}
}

func init() {

	typeKey := errbase.GetTypeKey(&DbosError{})

	errbase.RegisterLeafEncoder(typeKey, func(_ context.Context, err error) (string, []string, proto.Message) {
		dbosErr := err.(*DbosError)
		return dbosErr.Error(), encodeDbosErrorFields(dbosErr), nil
	})
	errbase.RegisterLeafDecoder(typeKey, func(_ context.Context, _ string, safeDetails []string, _ proto.Message) error {
		if decoded := decodeDbosErrorFields(safeDetails); decoded != nil {
			return decoded
		}
		return nil
	})

	errbase.RegisterWrapperEncoder(typeKey, func(_ context.Context, err error) (string, []string, proto.Message) {
		dbosErr := err.(*DbosError)
		return dbosErr.Error(), encodeDbosErrorFields(dbosErr), nil
	})
	errbase.RegisterWrapperDecoder(typeKey, func(_ context.Context, cause error, _ string, safeDetails []string, _ proto.Message) error {
		if decoded := decodeDbosErrorFields(safeDetails); decoded != nil {
			decoded.wrappedErr = cause
			return decoded
		}
		return nil
	})
}

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

// nil if the input is absent or cannot be decoded; callers should then fall

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

	if !errors.Is(decoded, context.Canceled) && cockroacherrors.Is(decoded, context.Canceled) {
		return &sentinelError{msg: decoded.Error(), sentinel: context.Canceled}
	}
	return decoded
}

type sentinelError struct {
	msg      string
	sentinel error
}

func (e *sentinelError) Error() string { return e.msg }
func (e *sentinelError) Unwrap() error { return e.sentinel }
