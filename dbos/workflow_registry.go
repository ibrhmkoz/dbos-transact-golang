package dbos

import (
	"fmt"
	"sync"
	"time"
)

type wrappedWorkflowFunc func(ctx DBOSContext, input any, inputSerialization string, opts ...WorkflowOption) (*WorkflowHandle[any], error)

// persistWorkflowDefinitions writes the execution limits of every worker-dispatched
// workflow to the system database. Called once at launch.
func (c *dbosContext) persistWorkflowDefinitions() error {
	for _, entry := range c.workflowRegistry.List(false) {
		if err := c.systemDB.upsertWorkflowDefinition(c, entry.Name, entry.GlobalConcurrency, entry.RateLimit, entry.Retention); err != nil {
			return fmt.Errorf("persist workflow definition %s: %w", entry.Name, err)
		}
	}
	return nil
}

// rateLimiter configures workflow execution rate limiting.
// Rate limits prevent overwhelming external services and provide backpressure.
type rateLimiter struct {
	limit  int           // Maximum number of workflows to start within the period
	period time.Duration // Time period for the rate limit
}

type WorkflowRegistryEntry struct {
	wrappedFunction wrappedWorkflowFunc
	MaxRetries      int
	Name            string
	FQN             string // Fully qualified name of the workflow function
	CronSchedule    string // Empty string for non-scheduled workflows

	// Execution limits applied when the workflow is claimed by a worker.
	GlobalConcurrency *int
	RateLimit         *rateLimiter
	Retention         time.Duration
}

type WorkflowRegistry struct {
	mu        sync.RWMutex
	store     map[string]WorkflowRegistryEntry
	fqnToName map[string]string
}

func NewWorkflowRegistry() *WorkflowRegistry {
	return &WorkflowRegistry{
		store:     make(map[string]WorkflowRegistryEntry),
		fqnToName: make(map[string]string),
	}
}

func (wf *WorkflowRegistry) Load(workflowName string) (WorkflowRegistryEntry, bool) {
	wf.mu.RLock()
	defer wf.mu.RUnlock()
	entry, exists := wf.store[workflowName]
	return entry, exists
}

func (wf *WorkflowRegistry) LoadOrStore(workflowName string, entry WorkflowRegistryEntry) (WorkflowRegistryEntry, bool) {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	if foundName, exists := wf.fqnToName[entry.FQN]; exists {
		return wf.store[foundName], true
	}
	if found, exists := wf.store[workflowName]; exists {
		return found, true
	}
	wf.store[workflowName] = entry
	wf.fqnToName[entry.FQN] = workflowName
	return entry, false
}

func (wf *WorkflowRegistry) ResolveName(workflowFQN string) (string, bool) {
	wf.mu.RLock()
	defer wf.mu.RUnlock()
	name, exists := wf.fqnToName[workflowFQN]
	return name, exists
}

func (wf *WorkflowRegistry) SetCronSchedule(workflowName, cronSchedule string) bool {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	entry, exists := wf.store[workflowName]
	if !exists {
		return false
	}

	entry.CronSchedule = cronSchedule
	wf.store[workflowName] = entry
	return true
}

// SetExecutionPolicies attaches persisted execution policies to a workflow.
func (wf *WorkflowRegistry) SetExecutionPolicies(workflowName string, globalConcurrency *int, rateLimit *rateLimiter, retention time.Duration) bool {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	entry, exists := wf.store[workflowName]
	if !exists {
		return false
	}

	entry.GlobalConcurrency = globalConcurrency
	entry.RateLimit = rateLimit
	entry.Retention = retention
	wf.store[workflowName] = entry
	return true
}

func (wf *WorkflowRegistry) Names() []string {
	wf.mu.RLock()
	defer wf.mu.RUnlock()
	names := make([]string, 0, len(wf.store))
	for name := range wf.store {
		names = append(names, name)
	}
	return names
}

func (wf *WorkflowRegistry) List(scheduledOnly bool) []WorkflowRegistryEntry {
	wf.mu.RLock()
	defer wf.mu.RUnlock()
	workflows := make([]WorkflowRegistryEntry, 0, len(wf.store))
	for _, workflow := range wf.store {
		if !scheduledOnly || workflow.CronSchedule != "" {
			workflows = append(workflows, workflow)
		}
	}
	return workflows
}

func (wf *WorkflowRegistry) Clear() {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	clear(wf.store)
	clear(wf.fqnToName)
}
