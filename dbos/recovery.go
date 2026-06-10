package dbos

func recoverPendingWorkflows(ctx *dbosContext, executorIDs []string) ([]*WorkflowHandle[any], error) {
	workflowHandles := make([]*WorkflowHandle[any], 0)
	// List pending workflows for the executors
	pendingWorkflows, err := retryWithResult(ctx, func() ([]WorkflowStatus, error) {
		appVersion := []string{}
		if ctx.applicationVersion != "" {
			appVersion = []string{ctx.applicationVersion}
		}
		return ctx.kernel.listWorkflows(ctx, listWorkflowsDBInput{
			status:             []WorkflowStatusType{WorkflowStatusPending},
			executorIDs:        executorIDs,
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
				return ctx.kernel.clearQueueAssignment(ctx, workflow.ID)
			}, withRetrierLogger(ctx.logger))
			if err != nil {
				ctx.logger.Error("Error clearing queue assignment for workflow", "workflow_id", workflow.ID, "name", workflow.Name, "error", err)
				continue
			}
			if cleared {
				workflowHandles = append(workflowHandles, newWorkflowHandle[any](ctx, workflow.ID))
			}
			continue
		}

		registeredWorkflow, exists := ctx.workflowRegistry.Load(workflow.Name)
		if !exists {
			ctx.logger.Error("Workflow function not found in registry", "workflow_id", workflow.ID, "name", workflow.Name)
			continue
		}

		// Convert workflow parameters to options.
		// Auth identity is re-attached so child workflows spawned during
		// recovery inherit the same identity as the original run.
		opts := []WorkflowOption{
			WithWorkflowID(workflow.ID),
			withIsRecovery(),
			WithAuthenticatedUser(workflow.AuthenticatedUser),
			WithAssumedRole(workflow.AssumedRole),
			WithAuthenticatedRoles(workflow.AuthenticatedRoles),
		}
		// Create a workflow context from the executor context
		// Pass encoded input directly - decoding will happen in workflow wrapper when we know the target type
		handle, err := registeredWorkflow.wrappedFunction(ctx, workflow.Input, workflow.Serialization, opts...)
		if err != nil {
			return nil, err
		}
		workflowHandles = append(workflowHandles, handle)
	}

	return workflowHandles, nil
}
