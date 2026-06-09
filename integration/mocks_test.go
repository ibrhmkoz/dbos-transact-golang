package dbos_test

import (
	"context"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

func step(ctx context.Context) (int, error) {
	return 1, nil
}

func childWorkflow(ctx dbos.DBOSContext, i int) (int, error) {
	return i + 1, nil
}

// Callables for the workflows, assigned in aRealProgramFunction before launch.
var (
	workflowWF      dbos.WorkflowDefinition[int, int]
	childWorkflowWF dbos.WorkflowDefinition[int, int]
)

func workflow(ctx dbos.DBOSContext, i int) (int, error) {
	// Test RunAsStep
	a, err := dbos.RunAsStep(ctx, step)
	if err != nil {
		return 0, err
	}

	// Child wf
	ch, err := childWorkflowWF(ctx, i)
	if err != nil {
		return 0, err
	}
	b, err := ch.GetResult()
	if err != nil {
		return 0, err
	}

	// Test messaging operations
	c, err := dbos.Recv[int](ctx, "chan1", 1*time.Second)
	if err != nil {
		return 0, err
	}
	d, err := dbos.GetEvent[int](ctx, "tgw", "event1", 1*time.Second)
	if err != nil {
		return 0, err
	}
	err = dbos.Send(ctx, "dst", 1, "topic")
	if err != nil {
		return 0, err
	}

	// Test SetEvent
	err = dbos.SetEvent(ctx, "test_key", "test_value")
	if err != nil {
		return 0, err
	}

	// Test Sleep
	_, err = dbos.Sleep(ctx, 100*time.Millisecond)
	if err != nil {
		return 0, err
	}

	// Test ID retrieval methods
	workflowID, err := ctx.GetWorkflowID()
	if err != nil {
		return 0, err
	}
	stepID, err := ctx.GetStepID()
	if err != nil {
		return 0, err
	}

	// Test workflow management
	_, err = dbos.RetrieveWorkflow[int](ctx, workflowID)
	if err != nil {
		return 0, err
	}

	err = dbos.CancelWorkflow(ctx, workflowID)
	if err != nil {
		return 0, err
	}

	_, err = dbos.ResumeWorkflow[int](ctx, workflowID)
	if err != nil {
		return 0, err
	}

	forkInput := dbos.ForkWorkflowInput{
		OriginalWorkflowID: workflowID,
		StartStep:          uint(stepID),
	}
	_, err = dbos.ForkWorkflow[int](ctx, forkInput)
	if err != nil {
		return 0, err
	}

	_, err = dbos.ListWorkflows(ctx)
	if err != nil {
		return 0, err
	}

	_, err = dbos.GetWorkflowSteps(ctx, workflowID)
	if err != nil {
		return 0, err
	}

	// Test accessor methods
	appVersion := ctx.GetApplicationVersion()
	executorID := ctx.GetExecutorID()
	appID := ctx.GetApplicationID()

	// Use some values to avoid compiler warnings
	_ = appVersion
	_ = executorID
	_ = appID

	// Test Go and Select methods (using stepAny to match Select signature)
	stepAny := func(ctx context.Context) (any, error) {
		return 1, nil
	}
	outcomeChan, err := dbos.Go(ctx, stepAny)
	if err != nil {
		return 0, err
	}

	// Test Select method
	e, err := dbos.Select(ctx, []<-chan dbos.StepOutcome[any]{outcomeChan})
	if err != nil {
		return 0, err
	}

	return a + b + c + d + e.(int), nil
}

func aRealProgramFunction(dbosCtx dbos.DBOSContext) error {

	childWorkflowWF = dbos.NewWorkflow(dbosCtx, childWorkflow)
	workflowWF = dbos.NewWorkflow(dbosCtx, workflow)

	err := dbos.Launch(dbosCtx)
	if err != nil {
		return err
	}
	defer dbos.Shutdown(dbosCtx, 1*time.Second)

	handle, err := workflowWF(dbosCtx, 2)
	if err != nil {
		return err
	}
	res, err := handle.GetResult()
	if err != nil {
		return err
	}
	if res != 5 {
		return fmt.Errorf("unexpected result: %v", res)
	}

	// Test WithValue
	valCtx := dbos.WithValue(dbosCtx, "key", "val")
	if valCtx == nil {
		return fmt.Errorf("WithValue returned nil")
	}

	// Test WithCancelCause
	cancelCtx, cf := dbos.WithCancelCause(dbosCtx)
	if cancelCtx == nil {
		return fmt.Errorf("WithCancelCause returned nil context")
	}
	if cf == nil {
		return fmt.Errorf("WithCancelCause returned nil cancel function")
	}

	return nil
}
