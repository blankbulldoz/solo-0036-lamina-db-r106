package engine

import (
	"crypto/rand"
	"encoding/hex"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"time"
)

type staged struct {
	Mutation   versions.Mutation
	InsertOnly bool
}
type Transaction struct {
	ID         string `json:"id"`
	Snapshot   uint64 `json:"snapshot"`
	ReadOnly   bool   `json:"read_only"`
	Started    string `json:"started"`
	committing bool
	writes     map[string]map[string]staged
}
type Commit struct {
	Sequence uint64 `json:"sequence"`
	Writes   int    `json:"writes"`
}

// TxnOutcome is the confirmation-ledger view of one transaction id. State is
// "committed", "active" (still open in this instance) or "unknown" (no durable
// commit and not active, e.g. rolled back or never confirmed). Reads after
// restart therefore answer from durable facts, never from hope.
type TxnOutcome struct {
	Transaction string `json:"transaction"`
	State       string `json:"state"`
	Sequence    uint64 `json:"sequence,omitempty"`
	Writes      int    `json:"writes,omitempty"`
}

func (d *DB) Begin(readOnly bool) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	if len(d.transactions) >= 128 {
		return Transaction{}, fault("capacity", "128 active transactions reached")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Transaction{}, err
	}
	tx := &Transaction{ID: hex.EncodeToString(id[:]), Snapshot: d.sequence, ReadOnly: readOnly, Started: time.Now().UTC().Format(time.RFC3339Nano), writes: map[string]map[string]staged{}}
	d.transactions[tx.ID] = tx
	out := *tx
	out.writes = nil
	return out, nil
}
func (d *DB) tx(id string) (*Transaction, error) {
	tx, ok := d.transactions[id]
	if !ok {
		return nil, fault("not_found", "transaction not active")
	}
	return tx, nil
}
func (d *DB) Put(id, table string, row datum.Row, insertOnly bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	if tx.committing {
		return fault("conflict", "transaction commit in progress")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	normalized, err := r.Normalize(row)
	if err != nil {
		return invalid(err)
	}
	key := r.Key(normalized)
	rows := d.store.Scan(table, tx.Snapshot)
	overlay(rows, tx.writes, table)
	if insertOnly {
		if _, ok := rows[key]; ok {
			return fault("conflict", "primary already exists")
		}
	}
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Row: normalized}, InsertOnly: insertOnly}
	return nil
}
func (tx *Transaction) reserve(table, key string) error {
	if _, exists := tx.writes[table][key]; exists {
		return nil
	}
	count := 0
	for _, m := range tx.writes {
		count += len(m)
	}
	if count >= 1000 {
		return fault("capacity", "transaction mutation limit 1000")
	}
	return nil
}
func (d *DB) Delete(id, table string, value datum.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	if tx.committing {
		return fault("conflict", "transaction commit in progress")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	col, _ := r.Column(r.Primary)
	if value.Null() || value.Kind != col.Type {
		return fault("invalid", "invalid primary value")
	}
	key := value.Key()
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Deleted: true}}
	return nil
}
func (d *DB) Rollback(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.committing {
		return fault("conflict", "transaction commit in progress")
	}
	delete(d.transactions, id)
	return nil
}

// Commit keeps the original single-transaction entry point. Internally the
// request joins the bounded group-commit pipeline so frames arriving together
// share one fsync, but this transaction receives its own result and is never
// bound to the fate of the rest of the group.
func (d *DB) Commit(id string) (Commit, error) {
	d.mu.Lock()
	if err := d.usable(); err != nil {
		d.mu.Unlock()
		return Commit{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		d.mu.Unlock()
		return Commit{}, err
	}
	if tx.committing {
		d.mu.Unlock()
		return Commit{}, fault("conflict", "transaction commit already in progress")
	}
	if len(flattenWrites(tx.writes)) == 0 {
		// Read-only/empty commit publishes nothing and consumes no sequence.
		delete(d.transactions, id)
		seq := tx.Snapshot
		d.mu.Unlock()
		return Commit{Sequence: seq}, nil
	}
	tx.committing = true
	job := d.buildCommitJob(tx)
	d.mu.Unlock()

	if err = d.committer.submit(job); err != nil {
		d.mu.Lock()
		if current, ok := d.transactions[id]; ok && current == tx {
			tx.committing = false
		}
		d.mu.Unlock()
		return Commit{}, err
	}
	return d.committer.wait(job)
}

// flattenWrites collects a transaction's private write set in deterministic
// order for validation and journal encoding.
func flattenWrites(byTable map[string]map[string]staged) []versions.Mutation {
	out := make([]versions.Mutation, 0)
	for _, mutations := range byTable {
		for _, m := range mutations {
			out = append(out, m.Mutation)
		}
	}
	return sortedMutations(out)
}

// buildCommitJob creates the pipeline job for one transaction. Validation runs
// later, under the lock, against published state plus the cumulative overlay of
// writes already accepted earlier in the same group.
func (d *DB) buildCommitJob(tx *Transaction) *commitJob {
	id := tx.ID
	job := &commitJob{kind: jobTransaction, txID: id}
	job.prepare = func(seq uint64, accepted map[string]map[string]staged) (journal.Record, error) {
		_ = seq
		writes := flattenWrites(tx.writes)
		// Same-key first-committer-wins against both durable commits and the
		// transactions accepted ahead of this one in the same group.
		for _, m := range writes {
			if d.store.Latest(m.Relation, m.Key) > tx.Snapshot {
				d.conflicts.Add(1)
				delete(d.transactions, id)
				return journal.Record{}, fault("conflict", "write conflict on %s", m.Relation)
			}
			if _, ok := accepted[m.Relation][m.Key]; ok {
				d.conflicts.Add(1)
				delete(d.transactions, id)
				return journal.Record{}, fault("conflict", "write conflict on %s", m.Relation)
			}
		}
		// Unique constraints are checked against the final state this commit
		// would produce, including earlier accepted writes of the same group.
		tables := map[string]bool{}
		for _, m := range writes {
			tables[m.Relation] = true
		}
		for table := range tables {
			if err := d.uniqueWithOverlay(table, tx.writes, accepted); err != nil {
				d.conflicts.Add(1)
				delete(d.transactions, id)
				return journal.Record{}, err
			}
		}
		// Accept: register into the group overlay so later group members contend
		// against the truly accepted state.
		for table, mutations := range tx.writes {
			if accepted[table] == nil {
				accepted[table] = map[string]staged{}
			}
			for key, m := range mutations {
				accepted[table][key] = m
			}
		}
		return journal.Record{Kind: "commit", Transaction: id, Writes: writes}, nil
	}
	job.applied = func(record journal.Record) {
		delete(d.transactions, id)
		d.commits.Add(1)
		job.result = Commit{Sequence: record.Sequence, Writes: len(record.Writes)}
	}
	return job
}

// TransactionOutcome reconciles one id against the durable journal and the live
// transaction table. It never reports success for work that has no durable
// record; callers holding an uncertain response use this after restart.
func (d *DB) TransactionOutcome(id string) (TxnOutcome, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return TxnOutcome{}, err
	}
	if commit, ok := reconcileOutcome(d.wal.Records, id); ok {
		return TxnOutcome{Transaction: id, State: "committed", Sequence: commit.Sequence, Writes: commit.Writes}, nil
	}
	if tx, ok := d.transactions[id]; ok {
		_ = tx
		return TxnOutcome{Transaction: id, State: "active"}, nil
	}
	return TxnOutcome{Transaction: id, State: "unknown"}, nil
}
