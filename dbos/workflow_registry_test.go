package dbos

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowRegistry(t *testing.T) {
	registry := NewWorkflowRegistry()
	first := WorkflowRegistryEntry{FQN: "first", MaxRetries: 1}
	second := WorkflowRegistryEntry{FQN: "second", Name: "custom-second", MaxRetries: 2}

	_, exists := registry.Load("missing")
	require.False(t, exists)

	stored, exists := registry.LoadOrStore(first.FQN, first)
	require.False(t, exists)
	first.Name = first.FQN
	assert.Equal(t, first, stored)

	found, exists := registry.Load(first.FQN)
	require.True(t, exists)
	assert.Equal(t, first, found)

	replacement := WorkflowRegistryEntry{FQN: first.FQN, MaxRetries: 100}
	found, exists = registry.LoadOrStore(first.FQN, replacement)
	require.True(t, exists)
	assert.Equal(t, first, found)

	_, exists = registry.LoadOrStore(second.Name, second)
	require.False(t, exists)
	require.False(t, registry.SetCronSchedule("missing", "0 * * * * *"))
	require.True(t, registry.SetCronSchedule(second.Name, "0 * * * * *"))

	name, exists := registry.ResolveName(first.FQN)
	require.True(t, exists)
	assert.Equal(t, first.FQN, name)
	name, exists = registry.ResolveName(second.FQN)
	require.True(t, exists)
	assert.Equal(t, second.Name, name)
	assert.ElementsMatch(t, []string{first.FQN, second.Name}, registry.Names())

	_, exists = registry.LoadOrStore("other-name", WorkflowRegistryEntry{FQN: first.FQN})
	require.True(t, exists)

	assert.ElementsMatch(t, []WorkflowRegistryEntry{
		first,
		{FQN: second.FQN, Name: second.Name, MaxRetries: second.MaxRetries, CronSchedule: "0 * * * * *"},
	}, registry.List(false))
	assert.Equal(t, []WorkflowRegistryEntry{
		{FQN: second.FQN, Name: second.Name, MaxRetries: second.MaxRetries, CronSchedule: "0 * * * * *"},
	}, registry.List(true))

	registry.Clear()
	assert.Empty(t, registry.List(false))
	assert.Empty(t, registry.Names())
	_, exists = registry.ResolveName(first.FQN)
	require.False(t, exists)
}
