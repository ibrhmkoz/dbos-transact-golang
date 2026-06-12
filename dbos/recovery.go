package dbos

func recoverPendingWorkflows(ctx *dbosContext, executorIds []string) ([]*WorkflowHandle[any], error) {
	workflowHandles := make([]*WorkflowHandle[any], 0)

	pendingWorkflows, err := retryWithResult(ctx, func() ([]WorkflowStatus, error) {
		appVersion := []string{}
		if ctx.applicationVersion != "" {
			appVersion = []string{ctx.applicationVersion}
		}
		return ctx.kernel.listWorkflows(ctx, listWorkflowsDBInput{
			status:             []WorkflowStatusType{WorkflowStatusPending},
			executorIds:        executorIds,
			applicationVersion: appVersion,
			loadInput:          true,
		})
	}, withRetrierLogger(ctx.logger))
	if err != nil {
		return nil, err
	}

	for _, workflow := range pendingWorkflows {
		if workflow.QueueName != "" {
			cleared, err := retryWithResult(ctx, func() (bool, error) {
				return ctx.kernel.clearQueueAssignment(ctx, workflow.Id)
			}, withRetrierLogger(ctx.logger))
			if err != nil {
				ctx.logger.Error("Error clearing queue assignment for workflow", "workflow_id", workflow.Id, "name", workflow.Name, "error", err)
				continue
			}
			if cleared {
				workflowHandles = append(workflowHandles, newWorkflowHandle[any](ctx, workflow.Id))
			}
			continue
		}

		registeredWorkflow, exists := ctx.workflowRegistry.Load(workflow.Name)
		if !exists {
			ctx.logger.Error("Workflow function not found in registry", "workflow_id", workflow.Id, "name", workflow.Name)
			continue
		}

		opts := []WorkflowOption{
			withWorkflowId(workflow.Id),
			withIsRecovery(),
			WithAuthenticatedUser(workflow.AuthenticatedUser),
			WithAssumedRole(workflow.AssumedRole),
			WithAuthenticatedRoles(workflow.AuthenticatedRoles),
		}

		handle, err := registeredWorkflow.wrappedFunction(ctx, workflow.Input, workflow.Serialization, opts...)
		if err != nil {
			return nil, err
		}
		workflowHandles = append(workflowHandles, handle)
	}

	return workflowHandles, nil
}
