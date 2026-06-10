package dbos

import (
	"sync"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func TestApplySchedulesInvalidSignature(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(10 * time.Second)

	require.NoError(t, dbosCtx.Launch())

	// Second argument is not ScheduledWorkflowInput.
	badInputType := func(ctx DBOSContext, input string) (any, error) { return nil, nil }
	err := ApplySchedules(dbosCtx, []ApplySchedulesRequest{
		{ScheduleName: "bad-input", WorkflowFn: badInputType, Schedule: "0 0 * * * *"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ScheduledWorkflowInput")

	// Not a function at all.
	err = ApplySchedules(dbosCtx, []ApplySchedulesRequest{
		{ScheduleName: "not-a-func", WorkflowFn: "not a function", Schedule: "0 0 * * * *"},
	})
	require.Error(t, err)

	// Too few parameters.
	tooFewParams := func(ctx DBOSContext) (any, error) { return nil, nil }
	err = ApplySchedules(dbosCtx, []ApplySchedulesRequest{
		{ScheduleName: "too-few", WorkflowFn: tooFewParams, Schedule: "0 0 * * * *"},
	})
	require.Error(t, err)

	// None of the above schedules should have been persisted.
	for _, name := range []string{"bad-input", "not-a-func", "too-few"} {
		s, err := GetSchedule(dbosCtx, name)
		require.NoError(t, err)
		require.Nil(t, s, "schedule %s should not have been created", name)
	}
}

func TestScheduleCronValidation(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(10 * time.Second)

	NewWorkflow(dbosCtx, testWorkflowForSchedule)
	require.NoError(t, dbosCtx.Launch())

	// CreateSchedule rejects a garbage cron expression up-front.
	err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: "bad-cron-create",
		Schedule:     "not a cron",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cron schedule")
	got, err := GetSchedule(dbosCtx, "bad-cron-create")
	require.NoError(t, err)
	require.Nil(t, got, "invalid-cron schedule must not be persisted")

	// ApplySchedules rejects invalid cron before writing any row (atomicity).
	err = ApplySchedules(dbosCtx, []ApplySchedulesRequest{
		{ScheduleName: "apply-good", WorkflowFn: testWorkflowForSchedule, Schedule: "0 0 * * * *"},
		{ScheduleName: "apply-bad", WorkflowFn: testWorkflowForSchedule, Schedule: "garbage"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cron schedule")
	for _, name := range []string{"apply-good", "apply-bad"} {
		s, err := GetSchedule(dbosCtx, name)
		require.NoError(t, err)
		require.Nil(t, s, "schedule %s should not have been created", name)
	}

	// Invalid timezone also surfaces at validate time.
	err = CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: "bad-tz",
		Schedule:     "0 0 * * * *",
	}, WithCronTimezone("Not/A_Zone"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cron schedule")
}

func TestBackfillSchedule(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(10 * time.Second)

	NewWorkflow(dbosCtx, testWorkflowForSchedule)

	err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: "backfill-schedule",
		Schedule:     "*/1 * * * * *", // Every second for testing
	})
	require.NoError(t, err)

	// Backfill last minute
	start := time.Now().Add(-1 * time.Minute)
	end := time.Now()

	ids, err := BackfillSchedule(dbosCtx, "backfill-schedule", start, end)
	require.NoError(t, err)

	// A `*/1 * * * * *` schedule over a one-minute window should enqueue
	// roughly 60 workflows; allow some slack for clock alignment.
	require.GreaterOrEqual(t, len(ids), 50, "backfill should have returned ~60 IDs, got %d", len(ids))
	backfilled, err := ListWorkflows(dbosCtx, WithWorkflowIDPrefix("sched-backfill-schedule-"))
	require.NoError(t, err)
	require.Equal(t, len(ids), len(backfilled), "returned IDs should match enqueued workflows")
	for _, wf := range backfilled {
		require.Equal(t, WorkflowStatusEnqueued, wf.Status)
	}

	// Idempotency: re-running the same backfill should not create duplicate rows
	// or bump recovery_attempts on the existing ones. Returned IDs should still
	// match the existing rows so callers can poll them.
	idsAgain, err := BackfillSchedule(dbosCtx, "backfill-schedule", start, end)
	require.NoError(t, err)
	require.Equal(t, len(ids), len(idsAgain), "second backfill must return the same IDs")
	again, err := ListWorkflows(dbosCtx, WithWorkflowIDPrefix("sched-backfill-schedule-"))
	require.NoError(t, err)
	require.Equal(t, len(backfilled), len(again), "second backfill must not enqueue duplicates")
	for _, wf := range again {
		require.Equal(t, 0, wf.Attempts, "second backfill must not bump recovery_attempts")
	}
}

// TestBackfillScheduleRecovery exercises the path where a backfilled workflow
// row is flipped to PENDING (simulating an executor crash mid-run) and then
// recovered via recoverPendingWorkflows. The recovered workflow must decode
// the ScheduledWorkflowInput written at backfill time and run it correctly.
func TestBackfillScheduleRecovery(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(10 * time.Second)

	scheduledInputCapture = sync.Map{}
	NewWorkflow(dbosCtx, testCapturingScheduledWorkflow)
	require.NoError(t, dbosCtx.Launch())

	// Use a far-future cron so the live scheduler doesn't fire while the test runs.
	const ctxValue = "backfill-recovery-context"
	const scheduleName = "backfill-recovery-schedule"
	err := CreateSchedule(dbosCtx, testCapturingScheduledWorkflow, CreateScheduleRequest{
		ScheduleName: scheduleName,
		Schedule:     "0 0 0 1 1 *", // Once a year
	}, WithScheduleContext(ctxValue))
	require.NoError(t, err)

	// Backfill a 5-second window of every-second ticks.
	start := time.Now().Add(-5 * time.Second).Truncate(time.Second)
	end := time.Now()
	c := dbosCtx.(*dbosContext)
	ids, err := c.kernel.backfillSchedule(c, backfillScheduleDBInput{
		ScheduleName: scheduleName,
		Schedule:     "*/1 * * * * *",
		StartTime:    start,
		EndTime:      end,
	})
	require.NoError(t, err)
	require.NotEmpty(t, ids, "backfill should have enqueued at least one workflow")

	target := ids[0]
	require.Eventually(t, func() bool {
		statuses, err := ListWorkflows(dbosCtx, WithWorkflowIDs([]string{target}))
		return err == nil && len(statuses) == 1 && statuses[0].Status == WorkflowStatusSuccess
	}, 10*time.Second, 50*time.Millisecond, "queue runner should run the backfilled workflow before recovery")

	// Drop the captured input from the first run so we can assert recovery's run populates it.
	scheduledInputCapture.Delete(target)

	setWorkflowStatusPending(t, dbosCtx, target)

	handles, err := recoverPendingWorkflows(c, []string{"local"})
	require.NoError(t, err)
	var recovered *WorkflowHandle[any]
	for _, h := range handles {
		if h.GetWorkflowID() == target {
			recovered = h
			break
		}
	}
	require.NotNil(t, recovered, "recovery should have produced a handle for %s", target)

	result, err := recovered.GetResult()
	require.NoError(t, err)
	require.Equal(t, "completed", result)

	captured, ok := scheduledInputCapture.Load(target)
	require.True(t, ok, "workflow should have captured its input on recovery")
	got := captured.(ScheduledWorkflowInput)
	require.Equal(t, ctxValue, got.Context, "Context should round-trip through DB-encoded inputs")
	require.False(t, got.ScheduledTime.IsZero(), "ScheduledTime should be populated from DB-encoded inputs")
	require.False(t, got.ScheduledTime.Before(start.Add(-time.Second)), "ScheduledTime should be within the backfill window")
	require.False(t, got.ScheduledTime.After(end.Add(time.Second)), "ScheduledTime should be within the backfill window")

	// CreateSchedule inside the workflow is step-wrapped: must exist exactly once after recovery.
	inner, err := ListSchedules(dbosCtx, WithScheduleNamePrefixes(target+"-inner"))
	require.NoError(t, err)
	require.Len(t, inner, 1)
}

func TestTriggerSchedule(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(10 * time.Second)

	scheduledInputCapture = sync.Map{}
	NewWorkflow(dbosCtx, testCapturingScheduledWorkflow)

	require.NoError(t, dbosCtx.Launch())

	const ctxValue = "trigger-context-value"
	err := CreateSchedule(dbosCtx, testCapturingScheduledWorkflow, CreateScheduleRequest{
		ScheduleName: "trigger-schedule",
		Schedule:     "0 0 * * * *",
	}, WithScheduleContext(ctxValue))
	require.NoError(t, err)

	beforeTrigger := time.Now()
	handle, err := TriggerSchedule(dbosCtx, "trigger-schedule")
	afterTrigger := time.Now()
	require.NoError(t, err)
	require.NotNil(t, handle)
	workflowID := handle.GetWorkflowID()
	require.NotEmpty(t, workflowID)
	require.Contains(t, workflowID, "trigger-schedule")

	result, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, "completed", result)

	captured, ok := scheduledInputCapture.Load(workflowID)
	require.True(t, ok, "workflow should have captured its input")
	got := captured.(ScheduledWorkflowInput)
	require.Equal(t, ctxValue, got.Context, "Context should match the schedule's configured context")
	require.False(t, got.ScheduledTime.Before(beforeTrigger.Add(-time.Second)), "ScheduledTime should be at or after the trigger call")
	require.False(t, got.ScheduledTime.After(afterTrigger.Add(time.Second)), "ScheduledTime should be at or before the trigger call returns")
}

func TestScheduleWithOptions(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	NewWorkflow(dbosCtx, testWorkflowForSchedule)

	err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: "full-options-schedule",
		Schedule:     "0 0 * * * *",
	},
		WithScheduleContext(map[string]string{"key": "value"}),
		WithAutomaticBackfill(true),
		WithCronTimezone("America/New_York"),
	)
	require.NoError(t, err)

	schedule, err := GetSchedule(dbosCtx, "full-options-schedule")
	require.NoError(t, err)
	require.True(t, schedule.AutomaticBackfill)
	require.Equal(t, "America/New_York", schedule.CronTimezone)
}

func testWorkflowForSchedule(ctx DBOSContext, input ScheduledWorkflowInput) (any, error) {
	return "completed", nil
}

func testWorkflowForScheduleCustomName(ctx DBOSContext, input ScheduledWorkflowInput) (any, error) {
	return "completed", nil
}

var scheduledInputCapture sync.Map

func testCapturingScheduledWorkflow(ctx DBOSContext, input ScheduledWorkflowInput) (any, error) {
	wfID, _ := GetWorkflowID(ctx)
	scheduledInputCapture.Store(wfID, input)
	// CreateSchedule is wrapped as a step via runAsTxn when called inside a
	// workflow. The inner cron never fires during tests.
	if err := CreateSchedule(ctx, testCapturingScheduledWorkflow, CreateScheduleRequest{
		ScheduleName: wfID + "-inner",
		Schedule:     "0 0 0 1 1 *",
	}); err != nil {
		return nil, err
	}
	return "completed", nil
}

var backfillRestartFiredEvent *Event

func testWorkflowForBackfillRestart(ctx DBOSContext, input ScheduledWorkflowInput) (any, error) {
	if backfillRestartFiredEvent != nil {
		backfillRestartFiredEvent.Set()
	}
	return "completed", nil
}

func TestAutomaticBackfillOnRestart(t *testing.T) {
	backfillRestartFiredEvent = NewEvent()

	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})

	NewWorkflow(dbosCtx, testWorkflowForBackfillRestart)
	require.NoError(t, dbosCtx.Launch())

	const scheduleName = "test-backfill-restart"
	const wfFQN = "github.com/dbos-inc/dbos-transact-golang/dbos.testWorkflowForBackfillRestart"

	err := CreateSchedule(dbosCtx, testWorkflowForBackfillRestart, CreateScheduleRequest{
		ScheduleName: scheduleName,
		Schedule:     "*/1 * * * * *", // Every second
	}, WithAutomaticBackfill(true))
	require.NoError(t, err)

	// Wait for the schedule to fire at least once so LastFiredAt is set.
	backfillRestartFiredEvent.Wait()

	// Snapshot how many runs have succeeded before the restart.
	var before []WorkflowStatus
	require.Eventually(t, func() bool {
		before, err = ListWorkflows(dbosCtx,
			WithName(wfFQN),
			WithStatus([]WorkflowStatusType{WorkflowStatusSuccess}),
		)
		return err == nil && len(before) >= 1
	}, 3*time.Second, 50*time.Millisecond, "expected at least one successful run before shutdown")

	dbosCtx.Shutdown(5 * time.Second)

	// Reset the event so the next Wait only returns after a post-restart fire.
	backfillRestartFiredEvent.Clear()

	// Simulate missed schedules while the context is down.
	time.Sleep(2 * time.Second)

	dbosCtx2 := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx2.Shutdown(5 * time.Second)

	NewWorkflow(dbosCtx2, testWorkflowForBackfillRestart)
	require.NoError(t, dbosCtx2.Launch())

	// Launch should backfill the missed runs; wait for one to execute.
	backfillRestartFiredEvent.Wait()

	// After backfill, the success count should have grown by more than one.
	require.Eventually(t, func() bool {
		after, err := ListWorkflows(dbosCtx2,
			WithName(wfFQN),
			WithStatus([]WorkflowStatusType{WorkflowStatusSuccess}),
		)
		return err == nil && len(after)-len(before) > 2
	}, 5*time.Second, 100*time.Millisecond, "expected backfill to produce more than one additional successful workflow")
}

func testWorkflowExpectingApplySchedulesError(ctx DBOSContext, _ string) (string, error) {
	err := ApplySchedules(ctx, []ApplySchedulesRequest{
		{ScheduleName: "x", WorkflowFn: testWorkflowForSchedule, Schedule: "0 0 * * * *"},
	})
	if err == nil {
		return "", nil
	}
	return err.Error(), nil
}

func testWorkflowExpectingBackfillScheduleError(ctx DBOSContext, _ string) (string, error) {
	_, err := BackfillSchedule(ctx, "any", time.Now().Add(-time.Minute), time.Now())
	if err == nil {
		return "", nil
	}
	return err.Error(), nil
}

func testWorkflowExpectingTriggerScheduleError(ctx DBOSContext, _ string) (string, error) {
	_, err := TriggerSchedule(ctx, "any")
	if err == nil {
		return "", nil
	}
	return err.Error(), nil
}

// TestScheduleWorkflowInternalRejections checks that ApplySchedules,
// BackfillSchedule, and TriggerSchedule reject calls from within a workflow.
func TestScheduleWorkflowInternalRejections(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(10 * time.Second)

	NewWorkflow(dbosCtx, testWorkflowForSchedule)
	applySchedulesWF := NewWorkflow(dbosCtx, testWorkflowExpectingApplySchedulesError)
	backfillScheduleWF := NewWorkflow(dbosCtx, testWorkflowExpectingBackfillScheduleError)
	triggerScheduleWF := NewWorkflow(dbosCtx, testWorkflowExpectingTriggerScheduleError)
	require.NoError(t, dbosCtx.Launch())

	cases := []struct {
		name string
		fn   Workflow[string, string]
		want string
	}{
		{"ApplySchedules", applySchedulesWF, "ApplySchedules cannot be called from within a workflow"},
		{"BackfillSchedule", backfillScheduleWF, "BackfillSchedule cannot be called from within a workflow"},
		{"TriggerSchedule", triggerScheduleWF, "TriggerSchedule cannot be called from within a workflow"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handle, err := tc.fn(dbosCtx, "")
			require.NoError(t, err)
			result, err := handle.GetResult()
			require.NoError(t, err)
			require.Contains(t, result, tc.want)
		})
	}
}

// TestScheduleCronTimezone verifies that a non-empty CronTimezone is applied
// to the installed cron entry via the CRON_TZ= prefix: Next() from a known
// wall-clock reference should fall at the configured hour in that tz.
func TestScheduleCronTimezone(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(5 * time.Second)

	NewWorkflow(dbosCtx, testWorkflowForSchedule)
	require.NoError(t, dbosCtx.Launch())

	const scheduleName = "tz-schedule"
	err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: scheduleName,
		Schedule:     "0 0 9 * * *", // 09:00:00 every day
	}, WithCronTimezone("America/New_York"))
	require.NoError(t, err)

	c := dbosCtx.(*dbosContext)
	var entry cron.Entry
	require.Eventually(t, func() bool {
		id, ok := c.installedScheduleEntryID(scheduleName)
		if !ok {
			return false
		}
		entry = c.getWorkflowScheduler().Entry(id)
		return entry.Schedule != nil
	}, 3*time.Second, 50*time.Millisecond, "reconciler should install the cron entry")

	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// 06:00 NY → next fire should be 09:00 NY the same day, regardless of
	// where the test host's local time sits.
	ref := time.Date(2025, 1, 15, 6, 0, 0, 0, loc)
	next := entry.Schedule.Next(ref).In(loc)
	require.Equal(t, 9, next.Hour(), "next fire should be 09:00 NY, got %v", next)
	require.Equal(t, 2025, next.Year())
	require.Equal(t, time.January, next.Month())
	require.Equal(t, 15, next.Day())
}
