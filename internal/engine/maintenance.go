package engine

import (
	"fmt"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
)

func (d *DB) image() snapshot.Image {
	return snapshot.Image{Format: 1, Sequence: d.sequence, Horizon: d.horizon, Catalog: d.catalog.Clone(), Store: d.store.Clone()}
}
func (d *DB) Checkpoint() (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return 0, err
	}
	if err := snapshot.Write(filepath.Join(d.dir, "checkpoint.lmi"), d.image()); err != nil {
		return 0, fault("unavailable", "checkpoint: %s", err)
	}
	return d.sequence, nil
}
func (d *DB) Backup() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, err
	}
	b, err := snapshot.Encode(d.image())
	if err != nil {
		return nil, err
	}
	if len(b) > snapshot.MaxImage {
		return nil, fault("capacity", "backup exceeds 128MiB")
	}
	return b, nil
}
func Restore(dir string, data []byte) error {
	image, err := snapshot.Decode(data)
	if err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("restore requires a new directory: %w", err)
	}
	installed := false
	defer func() {
		if !installed {
			os.Remove(filepath.Join(dir, "checkpoint.lmi"))
			os.Remove(filepath.Join(dir, "journal.lmw"))
			os.Remove(filepath.Join(dir, "lamina.lock"))
			os.Remove(dir)
		}
	}()
	w, err := journal.Open(dir, image.Sequence)
	if err != nil {
		return err
	}
	if err = snapshot.Write(filepath.Join(dir, "checkpoint.lmi"), image); err != nil {
		w.Close()
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	installed = true
	return nil
}
func (d *DB) Vacuum(requested uint64) (versions.VacuumResult, error) {
	d.mu.Lock()
	if err := d.usable(); err != nil {
		d.mu.Unlock()
		return versions.VacuumResult{}, err
	}
	if requested > d.sequence || requested < d.horizon {
		d.mu.Unlock()
		return versions.VacuumResult{}, fault("invalid", "vacuum horizon outside retained interval")
	}
	job := &commitJob{kind: jobVacuum}
	var result versions.VacuumResult
	var beforeVersions int
	job.prepare = func(seq uint64, accepted map[string]map[string]staged) (journal.Record, error) {
		_ = seq
		// Re-validate at durable time; the retained interval cannot regress.
		if requested > d.sequence || requested < d.horizon {
			return journal.Record{}, fault("invalid", "vacuum horizon outside retained interval")
		}
		horizon := requested
		// Only live, active transactions pin the horizon. Tentative writes not
		// yet durable are not a published fact and cannot advance the watermark.
		for _, tx := range d.transactions {
			if tx.Snapshot < horizon {
				horizon = tx.Snapshot
			}
		}
		_, beforeVersions = d.store.Count()
		return journal.Record{Kind: "vacuum", Horizon: horizon}, nil
	}
	job.applied = func(record journal.Record) {
		// apply() already pruned the store and advanced the watermark as part
		// of publishing the durable record; account for the change and rebuild
		// the historical index candidates.
		_, remaining := d.store.Count()
		d.indexes = index.Build(d.catalog, d.store)
		result = versions.VacuumResult{Horizon: record.Horizon, Removed: beforeVersions - remaining, Retained: remaining, Pinned: record.Horizon < requested}
	}
	d.mu.Unlock()

	if err := d.submitDurable(job); err != nil {
		return versions.VacuumResult{}, err
	}
	return result, nil
}
