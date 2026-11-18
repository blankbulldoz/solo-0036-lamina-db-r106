package engine

import (
	"errors"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"sync/atomic"
	"time"
)

// Bounded group commit configuration. The dwell is the maximum time a commit
// waits for peers so their frames can share one disk write and one fsync. The
// group size bounds how many records share a sync and the queue depth bounds
// how many waiting commits may occupy memory at once. Waiting longer improves
// sharing but raises low-load latency, queued memory and the blast radius of a
// failed sync; all three are therefore capped.
type CommitOptions struct {
	MaxDwell   time.Duration
	MaxGroup   int
	QueueDepth int
}

func DefaultCommitOptions() CommitOptions {
	return CommitOptions{MaxDwell: 2 * time.Millisecond, MaxGroup: 64, QueueDepth: 256}
}

// kind discriminates the durable work carried by one queue entry. Every entry
// still maps to exactly one journal record and one commit sequence; the group
// only shares the write/fsync, never the outcome.
const (
	jobTransaction = "transaction"
	jobRelation    = "relation"
	jobIndex       = "index"
	jobVacuum      = "vacuum"
)

type commitJob struct {
	kind string
	txID string

	// prepare runs under d.mu. seq is the sequence this record will take if
	// accepted. It validates against the already published state plus the writes
	// accepted earlier in the same group, and returns the record (sequence is
	// stamped by the pipeline). Returning an error rejects the job without
	// touching the journal.
	prepare func(seq uint64, accepted map[string]map[string]staged) (journal.Record, error)
	// applied runs under d.mu after the record is durable and published. It
	// performs in-memory side effects that must only happen on durable records
	// (index registration, vacuum accounting) and fills the job's result.
	applied func(record journal.Record)

	result Commit
	vacuum versions.VacuumResult
	done   chan struct{}
	err    error
}

// committer is a single background goroutine that drains the queue. Because it
// is the only writer that assigns sequences and appends to the journal, durable
// order is total and matches validation order.
type committer struct {
	db      *DB
	opts    CommitOptions
	queue   chan *commitJob
	wake    chan struct{}
	stopped chan struct{}

	queuedNow  atomic.Int64
	queuedPeak atomic.Int64
	groups     atomic.Uint64
	batched    atomic.Uint64
	rejected   atomic.Uint64
}

func newCommitter(db *DB, opts CommitOptions) *committer {
	if opts.MaxDwell <= 0 {
		opts.MaxDwell = DefaultCommitOptions().MaxDwell
	}
	if opts.MaxGroup <= 0 {
		opts.MaxGroup = DefaultCommitOptions().MaxGroup
	}
	if opts.QueueDepth <= 0 {
		opts.QueueDepth = DefaultCommitOptions().QueueDepth
	}
	c := &committer{
		db:      db,
		opts:    opts,
		queue:   make(chan *commitJob, opts.QueueDepth),
		wake:    make(chan struct{}, 1),
		stopped: make(chan struct{}),
	}
	go c.loop()
	return c
}

// submit enqueues a job with a hard memory bound. A full queue is a capacity
// rejection (429), not an indefinite wait or an optimistic success.
func (c *committer) submit(job *commitJob) error {
	job.done = make(chan struct{}, 1)
	select {
	case c.queue <- job:
	default:
		c.rejected.Add(1)
		return fault("capacity", "commit queue full; retry after backoff")
	}
	peak := c.queuedNow.Add(1)
	for {
		old := c.queuedPeak.Load()
		if peak <= old || c.queuedPeak.CompareAndSwap(old, peak) {
			break
		}
	}
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return nil
}

func (c *committer) wait(job *commitJob) (Commit, error) {
	<-job.done
	return job.result, job.err
}

// submitDurable enqueues a non-transaction durable job (DDL, vacuum) and blocks
// for its individual outcome. The caller must not hold d.mu.
func (d *DB) submitDurable(job *commitJob) error {
	if err := d.committer.submit(job); err != nil {
		return err
	}
	_, err := d.committer.wait(job)
	return err
}

func (c *committer) loop() {
	defer close(c.stopped)
	for {
		job, ok := <-c.queue
		if !ok {
			return
		}
		c.processGroup(job)
	}
}

// drain pulls everything currently queued, up to MaxGroup, without blocking for
// more arrivals; the first job is already in hand.
func (c *committer) drain(first *commitJob) []*commitJob {
	group := []*commitJob{first}
	for len(group) < c.opts.MaxGroup {
		select {
		case job := <-c.queue:
			group = append(group, job)
		default:
			return group
		}
	}
	return group
}

func (c *committer) processGroup(first *commitJob) {
	group := c.drain(first)

	// Bounded dwell: wait at most MaxDwell for more arrivals, then flush even
	// if no peer showed up. A single commit therefore never waits beyond the
	// configured bound.
	if len(group) < c.opts.MaxGroup {
		timer := time.NewTimer(c.opts.MaxDwell)
		select {
		case <-c.wake:
		case <-timer.C:
		}
		timer.Stop()
		for len(group) < c.opts.MaxGroup {
			select {
			case job := <-c.queue:
				group = append(group, job)
			default:
				goto ready
			}
		}
	}
ready:
	c.groups.Add(1)
	if len(group) > 1 {
		c.batched.Add(uint64(len(group) - 1))
	}

	// Phase 1: validate and assign sequences under the database lock. The
	// cumulative overlay lets each transaction see writes already accepted from
	// earlier transactions in this same group, so same-key and unique-value
	// contention is decided against the transactions truly accepted before it.
	records := make([]journal.Record, 0, len(group))
	accepted := map[string]map[string]staged{}
	acceptedJobs := make([]*commitJob, 0, len(group))
	c.db.mu.Lock()
	nextSeq := c.db.sequence + 1
	for _, job := range group {
		rec, err := job.prepare(nextSeq, accepted)
		if err != nil {
			job.err = err
			job.done <- struct{}{}
			c.queuedNow.Add(-1)
			c.rejected.Add(1)
			continue
		}
		// Sequences advance per accepted job within the group; rejected jobs
		// consume no sequence, and accepted sequences stay contiguous.
		rec.Sequence = nextSeq
		nextSeq++
		rec.Format = 1
		records = append(records, rec)
		acceptedJobs = append(acceptedJobs, job)
	}
	c.db.mu.Unlock()

	if len(records) == 0 {
		return
	}

	// Phase 2: one write and one fsync for the whole group, with no database
	// lock held so short reads can still progress against published state.
	appendErr := c.db.wal.AppendBatch(records)

	// Phase 3: publish outcomes under the lock. Nothing here is observable as a
	// fact until the frames are durable; checkpoints and vacuums take the same
	// lock and therefore can never capture a not-yet-durable tentative state.
	c.db.mu.Lock()
	if appendErr != nil {
		if errors.Is(appendErr, journal.ErrFrameTooLarge) {
			// Nothing reached the file: a deterministic capacity rejection. The
			// rejected transactions close without consuming a sequence, and the
			// instance stays usable for everyone else.
			tooLarge := fault("capacity", "commit exceeds journal frame limit")
			for _, job := range acceptedJobs {
				job.err = tooLarge
				job.done <- struct{}{}
				c.queuedNow.Add(-1)
				if job.kind == jobTransaction && job.txID != "" {
					delete(c.db.transactions, job.txID)
				}
			}
			c.db.mu.Unlock()
			return
		}
		// The write result is uncertain. Fail closed: poison the instance and
		// report every group member as uncertain rather than guessing. Each
		// transaction is still individually reconciled after restart.
		c.db.wal.Poisoned = true
		c.db.mu.Unlock()
		uncertain := fault("unavailable", "durable commit outcome uncertain; restart and reconcile: %s", appendErr)
		for _, job := range acceptedJobs {
			job.err = uncertain
			job.done <- struct{}{}
			c.queuedNow.Add(-1)
		}
		return
	}
	for i, job := range acceptedJobs {
		record := records[i]
		if err := c.db.apply(record); err != nil {
			// A durable record that cannot be applied is a local invariant
			// failure; poison so recovery reconciles the durable fact.
			c.db.wal.Poisoned = true
			c.db.mu.Unlock()
			cantApply := fault("unavailable", "durable record cannot be applied; restart required: %s", err)
			for _, j := range acceptedJobs[i:] {
				j.err = cantApply
				j.done <- struct{}{}
				c.queuedNow.Add(-1)
			}
			return
		}
		if len(record.Writes) > 0 {
			c.db.indexes.Apply(c.db.catalog, record.Writes)
		}
		job.applied(record)
		job.done <- struct{}{}
		c.queuedNow.Add(-1)
	}
	c.db.mu.Unlock()
}

// shutdown waits for queued work up to grace, then stops the loop. New submits
// must already be refused by the caller (DB marked closed). Any job not durably
// finished is released as uncertain rather than promised a result.
func (c *committer) shutdown(grace time.Duration) {
	deadline := time.NewTimer(grace)
	defer deadline.Stop()
	for len(c.queue) > 0 {
		select {
		case <-deadline.C:
			// Grace elapsed: stop accepting and fail whatever remains.
			close(c.queue)
			<-c.stopped
			for {
				select {
				case job := <-c.queue:
					job.err = fault("unavailable", "database closing before durable confirmation")
					select {
					case job.done <- struct{}{}:
					default:
					}
					c.queuedNow.Add(-1)
				default:
					return
				}
			}
		case <-time.After(time.Millisecond):
		}
	}
	close(c.queue)
	<-c.stopped
}

// reconcileOutcome looks a transaction id up in the durable journal. It is the
// confirmation ledger used after an uncertain result or a restart.
func reconcileOutcome(records []journal.Record, id string) (Commit, bool) {
	for _, r := range records {
		if r.Kind == "commit" && r.Transaction == id {
			return Commit{Sequence: r.Sequence, Writes: len(r.Writes)}, true
		}
	}
	return Commit{}, false
}

// sortedMutations orders a transaction's accepted mutations canonically.
func sortedMutations(writes []versions.Mutation) []versions.Mutation {
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Relation == writes[j].Relation {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Relation < writes[j].Relation
	})
	return writes
}
