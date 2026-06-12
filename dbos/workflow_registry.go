package dbos

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type wrappedWorkflowFunc func(ctx Context, input any, inputSerialization string, opts ...WorkflowOption) (*WorkflowHandle[any], error)

// computeDefinitionDigest fingerprints the code-declared configuration of a workflow.
// It deliberately covers conf only, not the function body: executable compatibility is
// tracked separately by application_version. A digest change means "declared conf
// changed" and moves the workflow_current pointer to a new immutable definition row.
func computeDefinitionDigest(e WorkflowRegistryEntry) string {
	fp := struct {
		Name                string
		InputSchema         string
		OutputSchema        string
		DebounceDelayMs     int64
		DebounceTimeoutMs   int64
		MaxRecoveryAttempts int
		GlobalConcurrency   *int
		RateLimitMax        int
		RateLimitPeriodMs   int64
		RetentionMs         int64
		CronSchedule        string
	}{
		Name:                e.Name,
		InputSchema:         e.InputSchema,
		OutputSchema:        e.OutputSchema,
		DebounceDelayMs:     e.DebounceDelay.Milliseconds(),
		DebounceTimeoutMs:   e.DebounceTimeout.Milliseconds(),
		MaxRecoveryAttempts: e.MaxRetries,
		GlobalConcurrency:   e.GlobalConcurrency,
		RetentionMs:         e.Retention.Milliseconds(),
		CronSchedule:        e.CronSchedule,
	}
	if e.RateLimit != nil {
		fp.RateLimitMax = e.RateLimit.limit
		fp.RateLimitPeriodMs = e.RateLimit.period.Milliseconds()
	}
	b, err := json.Marshal(fp)
	if err != nil {
		panic(fmt.Sprintf("failed to fingerprint workflow definition %s: %v", e.Name, err))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// persistWorkflowDefinitions reconciles registered workflows with the definitions
// table: insert the immutable definition row if its digest is unseen, then move the
// current pointer. Operator overrides are never touched.
func (c *dbosContext) persistWorkflowDefinitions() error {
	for _, entry := range c.workflowRegistry.List(false) {
		digest := computeDefinitionDigest(entry)
		c.workflowRegistry.SetDigest(entry.Name, digest)
		if err := c.kernel.reconcileWorkflowDefinition(c, entry, digest); err != nil {
			return fmt.Errorf("reconcile workflow definition %s: %w", entry.Name, err)
		}
	}
	return nil
}

type rateLimiter struct {
	limit  int
	period time.Duration
}

type WorkflowRegistryEntry struct {
	wrappedFunction wrappedWorkflowFunc
	MaxRetries      int
	Name            string
	FQN             string
	CronSchedule    string

	InputSchema     string
	OutputSchema    string
	DebounceDelay   time.Duration
	DebounceTimeout time.Duration
	Digest          string

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
	entry.Name = workflowName
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

func (wf *WorkflowRegistry) SetDigest(workflowName, digest string) bool {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	entry, exists := wf.store[workflowName]
	if !exists {
		return false
	}

	entry.Digest = digest
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
