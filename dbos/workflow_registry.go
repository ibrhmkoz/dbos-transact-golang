package dbos

import "sync"

type wrappedWorkflowFunc func(ctx DBOSContext, input any, inputSerialization string, opts ...WorkflowOption) (WorkflowHandle[any], error)

type WorkflowRegistryEntry struct {
	wrappedFunction wrappedWorkflowFunc
	MaxRetries      int
	Name            string
	FQN             string // Fully qualified name of the workflow function
	CronSchedule    string // Empty string for non-scheduled workflows
}

type WorkflowRegistry struct {
	store *sync.Map
}

func NewWorkflowRegistry() *WorkflowRegistry {
	return &WorkflowRegistry{store: &sync.Map{}}
}

func (wf *WorkflowRegistry) Load(workflowFQN string) (WorkflowRegistryEntry, bool) {
	entry, exists := wf.store.Load(workflowFQN)
	if !exists {
		return WorkflowRegistryEntry{}, false
	}

	return entry.(WorkflowRegistryEntry), true
}

func (wf *WorkflowRegistry) LoadOrStore(workflowFQN string, entry WorkflowRegistryEntry) (WorkflowRegistryEntry, bool) {
	found, exists := wf.store.LoadOrStore(workflowFQN, entry)
	if exists {
		return found.(WorkflowRegistryEntry), true
	}

	return entry, false
}

func (wf *WorkflowRegistry) SetCronSchedule(workflowFQN, cronSchedule string) bool {
	entry, exists := wf.Load(workflowFQN)
	if !exists {
		return false
	}

	entry.CronSchedule = cronSchedule
	wf.store.Store(workflowFQN, entry)
	return true
}

func (wf *WorkflowRegistry) List(scheduledOnly bool) []WorkflowRegistryEntry {
	var workflows []WorkflowRegistryEntry
	wf.store.Range(func(_, value any) bool {
		workflow := value.(WorkflowRegistryEntry)
		if !scheduledOnly || workflow.CronSchedule != "" {
			workflows = append(workflows, workflow)
		}
		return true
	})
	return workflows
}

func (wf *WorkflowRegistry) Clear() {
	wf.store.Clear()
}
