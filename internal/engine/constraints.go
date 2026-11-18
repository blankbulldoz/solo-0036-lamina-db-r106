package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/index"
	"lamina/internal/journal"
)

// unique validates unique indexes for a table against the durable published
// state plus one private write set (used by recovery validation where there is
// no in-flight group).
func (d *DB) unique(table string, writes map[string]map[string]staged) error {
	return d.uniqueWithOverlay(table, writes, nil)
}

// uniqueWithOverlay validates unique indexes against the durable state, one
// transaction's private writes, and the cumulative writes already accepted from
// earlier transactions in the same group. This keeps in-group unique-value
// contention consistent with the final published state.
func (d *DB) uniqueWithOverlay(table string, writes map[string]map[string]staged, accepted map[string]map[string]staged) error {
	rows := d.store.Scan(table, d.sequence)
	if accepted != nil {
		overlay(rows, accepted, table)
	}
	overlay(rows, writes, table)
	for _, i := range d.catalog.Indexes {
		if i.Relation == table && i.Unique {
			if err := checkUnique(rows, i); err != nil {
				return err
			}
		}
	}
	return nil
}
func checkUnique(rows map[string]datum.Row, i catalog.Index) error {
	values := map[string]string{}
	for key, row := range rows {
		v := row[i.Column]
		if v.Null() {
			continue
		}
		token := v.Key()
		if prior, exists := values[token]; exists && prior != key {
			return fault("conflict", "unique constraint %s violated", i.Name)
		}
		values[token] = key
	}
	return nil
}

func (d *DB) CreateRelation(r catalog.Relation) (catalog.Relation, error) {
	d.mu.Lock()
	if err := d.usable(); err != nil {
		d.mu.Unlock()
		return r, err
	}
	if err := r.Validate(); err != nil {
		d.mu.Unlock()
		return r, invalid(err)
	}
	if _, exists := d.catalog.Relations[r.Name]; exists {
		d.mu.Unlock()
		return r, fault("conflict", "relation exists")
	}
	job := &commitJob{
		kind: jobRelation,
		prepare: func(seq uint64, accepted map[string]map[string]staged) (journal.Record, error) {
			// Re-check inside the pipeline because another accepted job may
			// have created the same name while we queued.
			if _, exists := d.catalog.Relations[r.Name]; exists {
				return journal.Record{}, fault("conflict", "relation exists")
			}
			relation := r
			relation.Created = seq
			return journal.Record{Kind: "relation", Relation: &relation}, nil
		},
		applied: func(record journal.Record) {
			r = *record.Relation
		},
	}
	d.mu.Unlock()

	if err := d.submitDurable(job); err != nil {
		return r, err
	}
	return r, nil
}

func (d *DB) CreateIndex(i catalog.Index) (catalog.Index, error) {
	d.mu.Lock()
	if err := d.usable(); err != nil {
		d.mu.Unlock()
		return i, err
	}
	if err := d.catalog.CheckIndex(i); err != nil {
		d.mu.Unlock()
		return i, invalid(err)
	}
	if i.Unique {
		if err := checkUnique(d.store.Scan(i.Relation, d.sequence), i); err != nil {
			d.mu.Unlock()
			return i, err
		}
	}
	job := &commitJob{
		kind: jobIndex,
		prepare: func(seq uint64, accepted map[string]map[string]staged) (journal.Record, error) {
			if err := d.catalog.CheckIndex(i); err != nil {
				return journal.Record{}, invalid(err)
			}
			// Accepted in-group writes may have introduced a duplicate; verify
			// the final state this index would observe.
			if i.Unique {
				rows := d.store.Scan(i.Relation, d.sequence)
				overlay(rows, accepted, i.Relation)
				if err := checkUnique(rows, i); err != nil {
					d.conflicts.Add(1)
					return journal.Record{}, err
				}
			}
			indexDef := i
			indexDef.Created = seq
			return journal.Record{Kind: "index", Index: &indexDef}, nil
		},
		applied: func(record journal.Record) {
			i = *record.Index
			// Rebuild only after the index record is durable, so an
			// unconfirmed build never becomes a queryable candidate set.
			d.indexes = index.Build(d.catalog, d.store)
		},
	}
	d.mu.Unlock()

	if err := d.submitDurable(job); err != nil {
		return i, err
	}
	return i, nil
}
