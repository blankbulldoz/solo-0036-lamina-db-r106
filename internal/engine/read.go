package engine

import (
	"lamina/internal/datum"
	"lamina/internal/query"
)

func (d *DB) Query(q query.Request) (query.Result, error) {
	// A read lock is sufficient and deliberately does not block on a group
	// commit's disk sync: ordinary short reads keep progressing against the
	// last published sequence while frames are being flushed.
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return query.Result{}, err
	}
	r, err := d.catalog.Relation(q.Relation)
	if err != nil {
		return query.Result{}, invalid(err)
	}
	at := d.sequence
	var tx *Transaction
	if q.Transaction != "" {
		if q.At != nil {
			return query.Result{}, fault("invalid", "at and transaction are mutually exclusive")
		}
		tx, err = d.tx(q.Transaction)
		if err != nil {
			return query.Result{}, err
		}
		at = tx.Snapshot
	} else if q.At != nil {
		at = *q.At
	}
	if at < d.horizon || at > d.sequence {
		return query.Result{}, fault("snapshot", "snapshot unavailable; retained interval %d..%d", d.horizon, d.sequence)
	}
	if at < r.Created {
		return query.Result{}, fault("snapshot", "relation did not exist at snapshot")
	}
	if err = query.Bind(r, q); err != nil {
		return query.Result{}, invalid(err)
	}
	if tx != nil && len(tx.writes[q.Relation]) > 0 {
		q.ForceScan = true
	}
	plan, keys := query.Choose(d.catalog, d.indexes, q, at, len(d.store.Tables[q.Relation]))
	rows := map[string]datum.Row{}
	if plan.Access == "index" {
		for _, key := range keys {
			if row, exists := d.store.Read(q.Relation, key, at); exists {
				rows[key] = row
			}
		}
	} else {
		rows = d.store.Scan(q.Relation, at)
	}
	if tx != nil {
		overlay(rows, tx.writes, q.Relation)
	}
	result, err := query.Execute(r, q, rows, plan, keys)
	d.queries.Add(1)
	if err != nil {
		return result, invalid(err)
	}
	return result, nil
}
func (d *DB) Get(id, table string, key datum.Value) (datum.Row, error) {
	q := query.Request{Relation: table, Transaction: id, Limit: 1}
	d.mu.RLock()
	r, err := d.catalog.Relation(table)
	d.mu.RUnlock()
	if err != nil {
		return nil, invalid(err)
	}
	col, _ := r.Column(r.Primary)
	if key.Null() || key.Kind != col.Type {
		return nil, fault("invalid", "invalid primary value")
	}
	q.Where = []query.Predicate{{Column: r.Primary, Op: "=", Value: key}}
	result, err := d.Query(q)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return nil, fault("not_found", "tuple not found")
	}
	return result.Rows[0], nil
}
