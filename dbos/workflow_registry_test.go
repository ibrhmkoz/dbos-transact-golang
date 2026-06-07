package dbos

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowRegistry(t *testing.T) {
	registry := NewWorkflowRegistry()
	first := WorkflowRegistryEntry{FQN: "first", MaxRetries: 1}
	second := WorkflowRegistryEntry{FQN: "second", MaxRetries: 2}

	_, exists := registry.Load("missing")
	require.False(t, exists)

	stored, exists := registry.LoadOrStore(first.FQN, first)
	require.False(t, exists)
	assert.Equal(t, first, stored)

	found, exists := registry.Load(first.FQN)
	require.True(t, exists)
	assert.Equal(t, first, found)

	replacement := WorkflowRegistryEntry{FQN: first.FQN, MaxRetries: 100}
	found, exists = registry.LoadOrStore(first.FQN, replacement)
	require.True(t, exists)
	assert.Equal(t, first, found)

	_, exists = registry.LoadOrStore(second.FQN, second)
	require.False(t, exists)
	require.False(t, registry.SetCronSchedule("missing", "0 * * * * *"))
	require.True(t, registry.SetCronSchedule(second.FQN, "0 * * * * *"))

	assert.ElementsMatch(t, []WorkflowRegistryEntry{
		first,
		{FQN: second.FQN, MaxRetries: second.MaxRetries, CronSchedule: "0 * * * * *"},
	}, registry.List(false))
	assert.Equal(t, []WorkflowRegistryEntry{
		{FQN: second.FQN, MaxRetries: second.MaxRetries, CronSchedule: "0 * * * * *"},
	}, registry.List(true))

	registry.Clear()
	assert.Empty(t, registry.List(false))
}
