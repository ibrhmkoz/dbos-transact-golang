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

func childWorkflow(ctx dbos.Context, i int) (int, error) {
	return i + 1, nil
}

var (
	workflowWF      dbos.Workflow[int, int]
	childWorkflowWF dbos.Workflow[int, int]
)

func workflow(ctx dbos.Context, i int) (int, error) {

	a, err := dbos.Run(ctx, step)
	if err != nil {
		return 0, err
	}

	ch, err := childWorkflowWF(ctx, i)
	if err != nil {
		return 0, err
	}
	b, err := ch.GetResult()
	if err != nil {
		return 0, err
	}

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

	err = dbos.SetEvent(ctx, "test_key", "test_value")
	if err != nil {
		return 0, err
	}

	_, err = dbos.Sleep(ctx, 100*time.Millisecond)
	if err != nil {
		return 0, err
	}

	workflowId, err := ctx.GetWorkflowId()
	if err != nil {
		return 0, err
	}
	stepId, err := ctx.GetStepId()
	if err != nil {
		return 0, err
	}

	_, err = dbos.RetrieveWorkflow[int](ctx, workflowId)
	if err != nil {
		return 0, err
	}

	err = dbos.CancelWorkflow(ctx, workflowId)
	if err != nil {
		return 0, err
	}

	_, err = dbos.ResumeWorkflow[int](ctx, workflowId)
	if err != nil {
		return 0, err
	}

	forkInput := dbos.ForkWorkflowInput{
		OriginalWorkflowId: workflowId,
		StartStep:          uint(stepId),
	}
	_, err = dbos.ForkWorkflow[int](ctx, forkInput)
	if err != nil {
		return 0, err
	}

	_, err = dbos.ListWorkflows(ctx)
	if err != nil {
		return 0, err
	}

	_, err = dbos.GetWorkflowSteps(ctx, workflowId)
	if err != nil {
		return 0, err
	}

	appVersion := ctx.GetApplicationVersion()
	executorId := ctx.GetExecutorId()
	appId := ctx.GetApplicationId()

	// Use some values to avoid compiler warnings
	_ = appVersion
	_ = executorId
	_ = appId

	stepAny := func(ctx context.Context) (any, error) {
		return 1, nil
	}
	outcomeChan, err := dbos.Go(ctx, stepAny)
	if err != nil {
		return 0, err
	}

	e, err := dbos.Select(ctx, []<-chan dbos.StepOutcome[any]{outcomeChan})
	if err != nil {
		return 0, err
	}

	return a + b + c + d + e.(int), nil
}

func aRealProgramFunction(dbosCtx dbos.Context) error {

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

	valCtx := dbos.WithValue(dbosCtx, "key", "val")
	if valCtx == nil {
		return fmt.Errorf("WithValue returned nil")
	}

	cancelCtx, cf := dbos.WithCancelCause(dbosCtx)
	if cancelCtx == nil {
		return fmt.Errorf("WithCancelCause returned nil context")
	}
	if cf == nil {
		return fmt.Errorf("WithCancelCause returned nil cancel function")
	}

	return nil
}
