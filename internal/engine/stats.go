package engine

import (
	"sort"
	"time"
)

type Stats struct {
	Sequence       uint64           `json:"sequence"`
	Horizon        uint64           `json:"horizon"`
	Active         []Transaction    `json:"active"`
	Keys           int              `json:"keys"`
	Versions       int              `json:"versions"`
	IndexEntries   map[string]int   `json:"index_entries"`
	JournalRecords int              `json:"journal_records"`
	HistoryBase    uint64           `json:"history_base"`
	RecoveredTail  int64            `json:"recovered_tail_bytes"`
	Commits        uint64           `json:"commits_since_open"`
	Conflicts      uint64           `json:"conflicts_since_open"`
	Queries        uint64           `json:"queries_since_open"`
	CommitQueue    CommitQueueStats `json:"commit_queue"`
}

// CommitQueueStats exposes the bounded group-commit pipeline as observable
// facts: configuration bounds, current/peak occupancy and how much sharing the
// batching actually achieved.
type CommitQueueStats struct {
	MaxDwellMillis int    `json:"max_dwell_millis"`
	MaxGroup       int    `json:"max_group"`
	QueueDepth     int    `json:"queue_depth"`
	Queued         int64  `json:"queued"`
	PeakQueued     int64  `json:"peak_queued"`
	Groups         uint64 `json:"flush_groups"`
	BatchedRecords uint64 `json:"batched_records"`
	Rejected       uint64 `json:"rejected"`
}

func (d *DB) Stats() (Stats, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return Stats{}, err
	}
	keys, versions := d.store.Count()
	s := Stats{
		Sequence:       d.sequence,
		Horizon:        d.horizon,
		Keys:           keys,
		Versions:       versions,
		Active:         []Transaction{},
		IndexEntries:   map[string]int{},
		JournalRecords: len(d.wal.Records),
		HistoryBase:    d.wal.Base,
		RecoveredTail:  d.wal.RecoveredTail,
		Commits:        d.commits.Load(),
		Conflicts:      d.conflicts.Load(),
		Queries:        d.queries.Load(),
	}
	for _, tx := range d.transactions {
		copy := *tx
		copy.writes = nil
		s.Active = append(s.Active, copy)
	}
	sort.Slice(s.Active, func(i, j int) bool { return s.Active[i].ID < s.Active[j].ID })
	for name, t := range d.indexes.Trees {
		s.IndexEntries[name] = t.Size()
	}
	c := d.committer
	s.CommitQueue = CommitQueueStats{
		MaxDwellMillis: int(c.opts.MaxDwell / time.Millisecond),
		MaxGroup:       c.opts.MaxGroup,
		QueueDepth:     c.opts.QueueDepth,
		Queued:         c.queuedNow.Load(),
		PeakQueued:     c.queuedPeak.Load(),
		Groups:         c.groups.Load(),
		BatchedRecords: c.batched.Load(),
		Rejected:       c.rejected.Load(),
	}
	return s, nil
}
