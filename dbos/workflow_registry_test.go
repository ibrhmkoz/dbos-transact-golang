package dbos

import (
	"context"
	"testing"
	"time"

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

func TestWorkflowOverrides(t *testing.T) {
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	NewWorkflow(dbosCtx, simpleWorkflow, WithWorkflowName("override-target"), WithRateLimit(100, time.Minute))
	require.NoError(t, Start(dbosCtx))

	k := dbosCtx.(*dbosContext).kernel
	bg := context.Background()

	// Declared policy visible through the effective definition.
	def, err := k.queries.GetEffectiveWorkflowDefinition(bg, "override-target")
	require.NoError(t, err)
	require.NotNil(t, def.RateLimit)
	assert.EqualValues(t, 100, *def.RateLimit)

	// Operator override wins over the declared policy.
	limit := 5
	period := 30 * time.Second
	require.NoError(t, k.setWorkflowOverrides(bg, "override-target", WorkflowOverrides{RateLimit: &limit, RatePeriod: &period}))

	def, err = k.queries.GetEffectiveWorkflowDefinition(bg, "override-target")
	require.NoError(t, err)
	require.NotNil(t, def.RateLimit)
	assert.EqualValues(t, 5, *def.RateLimit)
	require.NotNil(t, def.RatePeriodMs)
	assert.EqualValues(t, period.Milliseconds(), *def.RatePeriodMs)
	// Concurrency was never overridden: declared value (none) still applies.
	assert.Nil(t, def.GlobalConcurrency)

	got, err := k.getWorkflowOverrides(bg, "override-target")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.RateLimit)
	assert.Equal(t, 5, *got.RateLimit)

	// Clearing restores the declared policy.
	require.NoError(t, k.clearWorkflowOverrides(bg, "override-target"))
	def, err = k.queries.GetEffectiveWorkflowDefinition(bg, "override-target")
	require.NoError(t, err)
	require.NotNil(t, def.RateLimit)
	assert.EqualValues(t, 100, *def.RateLimit)

	got, err = k.getWorkflowOverrides(bg, "override-target")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestResetWorkflow(t *testing.T) {
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	wf := NewWorkflow(dbosCtx, simpleWorkflow, WithWorkflowName("reset-target"))
	require.NoError(t, Start(dbosCtx))

	handle, err := wf(dbosCtx, "reset-input")
	require.NoError(t, err)
	original, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, "reset-input", original)

	// Reset = new instance, same input, fresh checkpoints, current app version.
	resetHandle, err := ResetWorkflow[string](dbosCtx, handle.GetWorkflowId())
	require.NoError(t, err)
	require.NotEqual(t, handle.GetWorkflowId(), resetHandle.GetWorkflowId(), "reset must create a new instance")

	result, err := resetHandle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "reset-input", result)

	// Original instance untouched.
	status, err := handle.GetStatus()
	require.NoError(t, err)
	assert.Equal(t, WorkflowStatusSuccess, status.Status)

	resetStatus, err := resetHandle.GetStatus()
	require.NoError(t, err)
	assert.Equal(t, handle.GetWorkflowId(), resetStatus.ForkedFrom)
	assert.Equal(t, dbosCtx.GetApplicationVersion(), resetStatus.ApplicationVersion)
}
