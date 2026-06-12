package dbos

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
)

type ScheduleStatus string

const (
	ScheduleStatusActive ScheduleStatus = "ACTIVE"
	ScheduleStatusPaused ScheduleStatus = "PAUSED"
)

type WorkflowSchedule struct {
	ScheduleId        string         `json:"schedule_id"`
	ScheduleName      string         `json:"schedule_name"`
	WorkflowName      string         `json:"workflow_name"`
	WorkflowClassName string         `json:"workflow_class_name,omitempty"`
	Schedule          string         `json:"schedule"`
	Status            ScheduleStatus `json:"status"`
	Context           any            `json:"context"`
	LastFiredAt       *time.Time     `json:"last_fired_at,omitempty"`
	AutomaticBackfill bool           `json:"automatic_backfill"`
	CronTimezone      string         `json:"cron_timezone,omitempty"`
	QueueName         string         `json:"queue_name,omitempty"`
}

// functions must accept. ScheduledTime is the cron tick time; Context carries

type ScheduledWorkflowInput struct {
	ScheduledTime time.Time `json:"scheduled_time"`
	Context       any       `json:"context,omitempty"`
}

type ApplySchedulesRequest struct {
	ScheduleName      string
	WorkflowFn        any
	Schedule          string
	Context           any
	AutomaticBackfill bool
	CronTimezone      string
}

const (
	_defaultSchedulePollInterval = 30 * time.Second
	_scheduleMaxJitter           = 10 * time.Second
)

func newScheduleCronParser() cron.Parser {
	return cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
}

func validateCronSchedule(spec, cronTimezone string) error {
	if spec == "" {
		return fmt.Errorf("schedule is required")
	}
	full := spec
	if cronTimezone != "" {
		full = "CRON_TZ=" + cronTimezone + " " + spec
	}
	if _, err := newScheduleCronParser().Parse(full); err != nil {
		return fmt.Errorf("invalid cron schedule %q: %w", spec, err)
	}
	return nil
}

func jitterCap(sched cron.Schedule, scheduledTime time.Time) time.Duration {
	if sched == nil {
		return 0
	}
	interval := sched.Next(scheduledTime).Sub(scheduledTime)
	if interval <= 0 {
		return 0
	}
	return min(interval/10, _scheduleMaxJitter)
}

// functions must conform to. Each tick the scheduler invokes the function

type ScheduledWorkflowFunc func(ctx DbosContext, input ScheduledWorkflowInput) (any, error)

func (c *dbosContext) addScheduleCronEntry(
	scheduleName, cronSchedule string,
	fn ScheduledWorkflowFunc,
	scheduleContext any,
) (cron.EntryID, error) {

	// an atomic to publish the entryID to that goroutine without a data race.
	var entryIdAtomic atomic.Int64
	assigned, err := c.getWorkflowScheduler().AddFunc(cronSchedule, func() {
		if !c.launched.Load() {
			return
		}
		entry := c.getWorkflowScheduler().Entry(cron.EntryID(entryIdAtomic.Load()))
		scheduledTime := entry.Prev
		if scheduledTime.IsZero() {
			scheduledTime = entry.Next
		}

		if cap := jitterCap(entry.Schedule, scheduledTime); cap > 0 {
			select {
			case <-time.After(rand.N(cap)):
			case <-c.Done():
				return
			}
		}

		input := ScheduledWorkflowInput{ScheduledTime: scheduledTime, Context: scheduleContext}
		if _, runErr := fn(c, input); runErr != nil {
			c.logger.Error("failed to run scheduled workflow", "schedule", scheduleName, "error", runErr)
		}
	})
	if err != nil {
		return 0, err
	}
	entryIdAtomic.Store(int64(assigned))
	return assigned, nil
}

func (c *dbosContext) buildDBScheduleFunc(schedule WorkflowSchedule) (ScheduledWorkflowFunc, error) {
	entry, ok := c.workflowRegistry.Load(schedule.WorkflowName)
	if !ok {
		return nil, fmt.Errorf("workflow not found: %s", schedule.WorkflowName)
	}
	wrappedFn := entry.wrappedFunction
	scheduleName := schedule.ScheduleName
	return func(ctx DbosContext, input ScheduledWorkflowInput) (any, error) {
		wfId := fmt.Sprintf("sched-%s-%s", scheduleName, input.ScheduledTime.Format(time.RFC3339))

		existing, err := retryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.kernel.listWorkflows(c, listWorkflowsDBInput{workflowIds: []string{wfId}})
		}, withRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("failed to check existing scheduled workflow", "schedule", scheduleName, "workflow_id", wfId, "error", err)
			return nil, err
		}
		if len(existing) > 0 {
			c.logger.Debug("skipping schedule tick", "schedule", scheduleName, "scheduledTime", input.ScheduledTime)
			return nil, nil
		}

		ser := resolveEncoder(ctx)
		encodedInput, err := ser.Encode(input)
		if err != nil {
			return nil, fmt.Errorf("failed to encode scheduled workflow input: %w", err)
		}

		opts := []WorkflowOption{
			withWorkflowId(wfId),
			withWorkflowName(entry.FQN),
		}
		// Scheduled workflows always run against the latest registered application version, so a stale executor does not pick them up after a new deploy.
		latest, err := retryWithResult(c, func() (*VersionInfo, error) {
			return c.kernel.getLatestApplicationVersion(c)
		}, withRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("failed to fetch latest application version for scheduled workflow", "schedule", scheduleName, "workflow_id", wfId, "error", err)
		} else if latest != nil {
			opts = append(opts, WithApplicationVersion(latest.Name))
		}
		result, runErr := wrappedFn(ctx, encodedInput, ser.Name(), opts...)

		if err := retry(c, func() error {
			return c.kernel.updateScheduleLastFiredAt(c, scheduleName, time.Now())
		}, withRetrierLogger(c.logger)); err != nil {
			c.logger.Error("failed to update schedule last fired time after retries", "schedule", scheduleName, "error", err)
		}

		return result, runErr
	}, nil
}

func (c *dbosContext) addDBScheduleToScheduler(schedule WorkflowSchedule) {
	fn, err := c.buildDBScheduleFunc(schedule)
	if err != nil {
		c.logger.Error("failed to get workflow for schedule", "schedule", schedule.ScheduleName, "error", err)
		return
	}

	spec := schedule.Schedule
	if schedule.CronTimezone != "" {
		spec = "CRON_TZ=" + schedule.CronTimezone + " " + spec
	}

	entryId, err := c.addScheduleCronEntry(schedule.ScheduleName, spec, fn, schedule.Context)
	if err != nil {
		c.logger.Error("failed to add schedule to scheduler", "schedule", schedule.ScheduleName, "error", err)
		return
	}

	c.scheduleMu.Lock()
	c.scheduleEntryIds[schedule.ScheduleName] = entryId
	c.scheduleInstalledIds[schedule.ScheduleName] = schedule.ScheduleId
	c.scheduleMu.Unlock()
	c.logger.Info("Added schedule to scheduler", "schedule", schedule.ScheduleName, "workflow", schedule.WorkflowName)
}

func (c *dbosContext) installedScheduleEntryId(scheduleName string) (cron.EntryID, bool) {
	c.scheduleMu.Lock()
	defer c.scheduleMu.Unlock()
	id, ok := c.scheduleEntryIds[scheduleName]
	return id, ok
}

func (c *dbosContext) removeDBScheduleFromScheduler(scheduleName string) {
	c.scheduleMu.Lock()
	entryId, exists := c.scheduleEntryIds[scheduleName]
	if exists {
		delete(c.scheduleEntryIds, scheduleName)
		delete(c.scheduleInstalledIds, scheduleName)
	}
	c.scheduleMu.Unlock()
	if !exists {
		c.logger.Warn("attempted to remove non-existent schedule from scheduler", "schedule", scheduleName)
		return
	}
	c.getWorkflowScheduler().Remove(entryId)
	c.logger.Info("Removed schedule from scheduler", "schedule", scheduleName)
}

func (c *dbosContext) runScheduleReconciler() {
	interval := c.config.SchedulerPollingInterval
	if interval <= 0 {
		interval = _defaultSchedulePollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		c.reconcileSchedules()

		select {
		case <-c.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *dbosContext) reconcileSchedules() {
	schedules, err := c.kernel.listSchedules(c, listSchedulesDBInput{})
	if err != nil {
		c.logger.Warn("failed to list schedules for reconciler", "error", err)
		return
	}

	current := make(map[string]*WorkflowSchedule, len(schedules))
	for i := range schedules {
		current[schedules[i].ScheduleName] = &schedules[i]
	}

	// Collect names first to avoid mutating the map while iterating.
	var toRemove []string
	c.scheduleMu.Lock()
	for name := range c.scheduleEntryIds {
		sched, ok := current[name]
		if !ok || sched.Status != ScheduleStatusActive {
			toRemove = append(toRemove, name)
			continue
		}
		if c.scheduleInstalledIds[name] != sched.ScheduleId {
			toRemove = append(toRemove, name)
		}
	}
	c.scheduleMu.Unlock()
	for _, name := range toRemove {
		c.removeDBScheduleFromScheduler(name)
	}

	for name, sched := range current {
		if sched.Status != ScheduleStatusActive {
			continue
		}
		c.scheduleMu.Lock()
		_, exists := c.scheduleEntryIds[name]
		c.scheduleMu.Unlock()
		if exists {
			continue
		}

		if sched.AutomaticBackfill && sched.LastFiredAt != nil {
			start := sched.LastFiredAt.Add(time.Second)
			end := time.Now()
			if start.Before(end) {
				c.logger.Info("performing automatic backfill", "schedule", sched.ScheduleName, "start", start, "end", end)
				if _, err := c.kernel.backfillSchedule(c, backfillScheduleDBInput{
					ScheduleName: sched.ScheduleName,
					Schedule:     sched.Schedule,
					StartTime:    start,
					EndTime:      end,
				}); err != nil {
					c.logger.Error("automatic backfill failed", "schedule", sched.ScheduleName, "error", err)
				}
			}
		}

		c.addDBScheduleToScheduler(*sched)
	}
}
