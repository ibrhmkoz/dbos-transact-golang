package dbos

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

const (
	nilMarker = "__DBOS_NIL"

	PortableSerializerName = "portable_json"
)

// Custom serializers implement Serializer[any] and must embed type info in payloads (e.g., using a type envelope)
type Serializer[T any] interface {
	Name() string

	Encode(data T) (*string, error)

	Decode(data *string) (T, error)
}

type jsonSerializer[T any] struct {
	portable bool
}

func newJsonSerializer[T any]() Serializer[T] {
	return &jsonSerializer[T]{portable: false}
}

func newPortableSerializer[T any]() Serializer[T] {
	return &jsonSerializer[T]{portable: true}
}

func (j *jsonSerializer[T]) Name() string {
	if j.portable {
		return PortableSerializerName
	}
	return "DBOS_JSON"
}

func (j *jsonSerializer[T]) Encode(data T) (*string, error) {
	if isNilValue(data) {
		if j.portable {
			s := "null"
			return &s, nil
		}
		marker := string(nilMarker)
		return &marker, nil
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to encode data: %w", err)
	}

	if j.portable {
		s := string(jsonBytes)
		return &s, nil
	}
	encodedStr := base64.StdEncoding.EncodeToString(jsonBytes)
	return &encodedStr, nil
}

func (j *jsonSerializer[T]) Decode(data *string) (T, error) {
	if j.portable {
		if data == nil || *data == "null" {
			return getNilOrZeroValue[T](), nil
		}
		var result T
		if err := json.Unmarshal([]byte(*data), &result); err != nil {
			return result, fmt.Errorf("failed to decode portable json data: %w", err)
		}
		return result, nil
	}

	if data == nil || *data == nilMarker {
		return getNilOrZeroValue[T](), nil
	}

	var result T
	dataBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return result, fmt.Errorf("failed to decode base64 data: %w", err)
	}

	if err := json.Unmarshal(dataBytes, &result); err != nil {
		return result, fmt.Errorf("failed to decode json data: %w", err)
	}

	return result, nil
}

// Users must call gob.Register(ConcreteType{}) for each concrete type

type GobSerializer struct{}

func NewGobSerializer() Serializer[any] {
	return &GobSerializer{}
}

func (g *GobSerializer) Name() string {
	return "DBOS_GOB"
}

func (g *GobSerializer) Encode(data any) (*string, error) {
	if isNilValue(data) {
		marker := string(nilMarker)
		return &marker, nil
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&data); err != nil {
		return nil, fmt.Errorf("failed to gob encode data: %w", err)
	}
	encodedStr := base64.StdEncoding.EncodeToString(buf.Bytes())
	return &encodedStr, nil
}

func (g *GobSerializer) Decode(data *string) (any, error) {
	if data == nil || *data == nilMarker {
		return nil, nil
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 data: %w", err)
	}

	var result any
	dec := gob.NewDecoder(bytes.NewReader(decodedBytes))
	if err := dec.Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to gob decode data: %w", err)
	}

	return result, nil
}

type typedCustomSerializerAdapter[T any] struct {
	inner Serializer[any]
}

func (a *typedCustomSerializerAdapter[T]) Name() string {
	return a.inner.Name()
}

func (a *typedCustomSerializerAdapter[T]) Encode(data T) (*string, error) {
	return a.inner.Encode(data)
}

func (a *typedCustomSerializerAdapter[T]) Decode(data *string) (T, error) {
	decoded, err := a.inner.Decode(data)
	if err != nil {
		return *new(T), err
	}
	if decoded == nil {
		return getNilOrZeroValue[T](), nil
	}
	typed, ok := decoded.(T)
	if !ok {
		return *new(T), fmt.Errorf("custom serializer returned %T, expected %T", decoded, *new(T))
	}
	return typed, nil
}

type PortableWorkflowArgs struct {
	PositionalArgs []any          `json:"positionalArgs"`
	NamedArgs      map[string]any `json:"namedArgs"`
}

type portableArgsRaw struct {
	PositionalArgs []json.RawMessage `json:"positionalArgs"`
	NamedArgs      map[string]any    `json:"namedArgs"`
}

func encodePortableArgs(data any) (*string, error) {
	var toEncode any
	if _, ok := data.(PortableWorkflowArgs); ok {
		toEncode = data
	} else {
		argBytes, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal portable arg: %w", err)
		}
		toEncode = portableArgsRaw{
			PositionalArgs: []json.RawMessage{argBytes},
			NamedArgs:      map[string]any{},
		}
	}
	return newPortableSerializer[any]().Encode(toEncode)
}

func decodePortableArgs[T any](data *string) (T, error) {
	if data == nil || *data == "null" {
		return getNilOrZeroValue[T](), nil
	}

	if reflect.TypeFor[T]() == reflect.TypeFor[PortableWorkflowArgs]() {
		var result T
		if err := json.Unmarshal([]byte(*data), &result); err != nil {
			return *new(T), fmt.Errorf("failed to decode portable args envelope as %T: %w", *new(T), err)
		}
		return result, nil
	}
	var envelope portableArgsRaw
	if err := json.Unmarshal([]byte(*data), &envelope); err != nil {
		return *new(T), fmt.Errorf("failed to unmarshal portable args envelope: %w", err)
	}
	if len(envelope.PositionalArgs) == 0 {
		return getNilOrZeroValue[T](), nil
	}
	var result T
	if err := json.Unmarshal(envelope.PositionalArgs[0], &result); err != nil {
		return *new(T), fmt.Errorf("failed to unmarshal portable arg into %T: %w", *new(T), err)
	}
	return result, nil
}

func resolveEncoder(ctx context.Context) Serializer[any] {
	if wfState, ok := ctx.Value(workflowStateKey).(*workflowState); ok && wfState != nil && wfState.isPortableWorkflow {
		return newPortableSerializer[any]()
	}
	if dc, ok := ctx.(*dbosContext); ok && dc.serializer != nil {
		return dc.serializer
	}
	return newJsonSerializer[any]()
}

func resolveDecoder[T any](storedSerialization string, customSer Serializer[any]) (Serializer[T], error) {
	if storedSerialization == PortableSerializerName {
		return newPortableSerializer[T](), nil
	}
	if customSer != nil && customSer.Name() == storedSerialization {
		return &typedCustomSerializerAdapter[T]{inner: customSer}, nil
	}
	if storedSerialization == "" || storedSerialization == "DBOS_JSON" {
		return newJsonSerializer[T](), nil
	}
	return nil, fmt.Errorf("unknown serialization format %q", storedSerialization)
}

func getCustomSerializerFromCtx(ctx Context) Serializer[any] {
	if dc, ok := ctx.(*dbosContext); ok {
		return dc.serializer
	}
	return nil
}

func isNilValue(v any) bool {
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return true
	}
	switch val.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func, reflect.Interface:
		return val.IsNil()
	}
	return false
}

func getNilOrZeroValue[T any]() T {
	var result T
	resultType := reflect.TypeOf(result)
	if resultType == nil {
		return result
	}

	if resultType.Kind() == reflect.Pointer {
		return reflect.Zero(resultType).Interface().(T)
	}
	// Otherwise return zero value
	return result
}

type PortableWorkflowError struct {
	Name    string `json:"name"`
	Message string `json:"message"`
	Code    any    `json:"code,omitempty"`
	Data    any    `json:"data,omitempty"`
}

func (e *PortableWorkflowError) Error() string {
	return e.Message
}

func serializeWorkflowError(err error, serialization string) string {
	if serialization != PortableSerializerName {
		return err.Error()
	}
	var errData PortableWorkflowError
	if pe := (*PortableWorkflowError)(nil); errors.As(err, &pe) {
		errData = *pe
	} else {
		errData = PortableWorkflowError{
			Name:    "Portable Error",
			Message: err.Error(),
		}
	}
	b, jsonErr := json.Marshal(errData)
	if jsonErr != nil {
		return err.Error()
	}
	return string(b)
}

func deserializeWorkflowError(errStr *string, errEncoded *string, serialization string) error {
	if errStr == nil || *errStr == "" {
		return nil
	}
	if serialization != PortableSerializerName {
		if decoded := decodeWorkflowError(errEncoded); decoded != nil {
			return decoded
		}
		return errors.New(*errStr)
	}
	var pe PortableWorkflowError
	if err := json.Unmarshal([]byte(*errStr), &pe); err != nil {
		return errors.New(*errStr)
	}
	return &pe
}
