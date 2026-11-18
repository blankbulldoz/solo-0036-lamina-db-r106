package journal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const MaxFrame = 16 << 20

type WAL struct {
	Base          uint64
	file          *os.File
	lock          *os.File
	Records       []Record
	RecoveredTail int64
	Poisoned      bool
}

func Open(dir string, base uint64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lamina.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("data directory already open: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "journal.lmw"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		lock.Close()
		return nil, err
	}
	w := &WAL{file: f, lock: lock, Base: base}
	info, err := f.Stat()
	if err != nil {
		w.Close()
		return nil, err
	}
	if info.Size() == 0 {
		header := make([]byte, 16)
		copy(header[:4], "LMW1")
		binary.BigEndian.PutUint64(header[4:12], base)
		binary.BigEndian.PutUint32(header[12:], crc32.ChecksumIEEE(header[:12]))
		if _, err = f.Write(header); err == nil {
			err = f.Sync()
		}
		if err != nil {
			w.Close()
			return nil, err
		}
	}
	directory, syncErr := os.Open(dir)
	if syncErr != nil {
		w.Close()
		return nil, syncErr
	}
	syncErr = directory.Sync()
	directory.Close()
	if syncErr != nil {
		w.Close()
		return nil, syncErr
	}
	if _, err = f.Seek(0, 0); err != nil {
		w.Close()
		return nil, err
	}
	if err = w.recover(); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}
func (w *WAL) recover() error {
	var header [16]byte
	if _, err := io.ReadFull(w.file, header[:]); err != nil {
		return fmt.Errorf("journal header: %w", err)
	}
	if string(header[:4]) != "LMW1" || crc32.ChecksumIEEE(header[:12]) != binary.BigEndian.Uint32(header[12:]) {
		return fmt.Errorf("invalid journal header")
	}
	actualBase := binary.BigEndian.Uint64(header[4:12])
	if actualBase > w.Base {
		return fmt.Errorf("journal requires newer checkpoint")
	}
	w.Base = actualBase
	offset := int64(16)
	last := w.Base
	for {
		var head [8]byte
		n, err := io.ReadFull(w.file, head[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			w.RecoveredTail = int64(n)
			break
		}
		if err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(head[:4])
		if length == 0 || length > MaxFrame {
			return fmt.Errorf("invalid journal length at %d", offset)
		}
		payload := make([]byte, length)
		n, err = io.ReadFull(w.file, payload)
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			w.RecoveredTail = int64(8 + n)
			break
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(head[4:]) {
			return fmt.Errorf("journal checksum mismatch at %d", offset)
		}
		var r Record
		if err = json.Unmarshal(payload, &r); err != nil {
			return fmt.Errorf("journal json at %d: %w", offset, err)
		}
		if r.Format != 1 || r.Sequence != last+1 {
			return fmt.Errorf("journal sequence or format mismatch at %d", offset)
		}
		w.Records = append(w.Records, r)
		last = r.Sequence
		offset += int64(8 + length)
	}
	if w.RecoveredTail > 0 {
		if err := w.file.Truncate(offset); err != nil {
			return err
		}
		if err := w.file.Sync(); err != nil {
			return err
		}
	}
	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}

var ErrFrameTooLarge = fmt.Errorf("commit exceeds journal frame limit")

// Append durably writes one record with its own fsync. It remains for callers
// that need a single-record write; the engine normally uses AppendBatch so that
// records arriving together share one write and one fsync.
func (w *WAL) Append(r Record) error {
	return w.AppendBatch([]Record{r})
}

// AppendBatch encodes and durably writes several contiguous records with a
// single write and a single fsync. The frame sequence numbers must be
// contiguous and continue the journal. On a write or sync failure the WAL is
// poisoned: the outcome is uncertain and the owning instance must stop serving
// and recover from the journal before accepting more work. Pre-write failures
// (encoding or frame budget) leave the WAL untouched and are reported without
// poisoning because nothing reached the file.
func (w *WAL) AppendBatch(records []Record) error {
	if w.Poisoned {
		return fmt.Errorf("journal is unavailable; restart required")
	}
	if len(records) == 0 {
		return nil
	}
	expected := w.Base + uint64(len(w.Records)) + 1
	frames := make([][]byte, 0, len(records))
	total := 0
	for _, r := range records {
		if r.Sequence != expected {
			return fmt.Errorf("noncontiguous journal sequence")
		}
		expected++
		r.Format = 1
		payload, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if len(payload) > MaxFrame {
			return ErrFrameTooLarge
		}
		frame := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
		copy(frame[8:], payload)
		frames = append(frames, frame)
		total += len(frame)
	}
	combined := make([]byte, 0, total)
	for _, frame := range frames {
		combined = append(combined, frame...)
	}
	if _, err := w.file.Write(combined); err != nil {
		w.Poisoned = true
		return err
	}
	if err := w.file.Sync(); err != nil {
		w.Poisoned = true
		return err
	}
	w.Records = append(w.Records, records...)
	return nil
}
func (w *WAL) Close() error {
	err := w.file.Close()
	w.lock.Close()
	return err
}
