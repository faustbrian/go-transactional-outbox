package postgres

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/faustbrian/go-transactional-outbox/v2"
)

func TestPreparedInsertOwnsEnvelopeStorage(t *testing.T) {
	w, err := NewWriter(WriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	envelope := outbox.Envelope{ID: "effect-a", Topic: "identity.created.v1", Payload: []byte("safe"), PayloadVersion: 1, Metadata: map[string]string{"tenant": "tenant-a"}, CreatedAt: now, AvailableAt: now}
	p, err := w.PrepareInsert([]outbox.Envelope{envelope})
	if err != nil {
		t.Fatal(err)
	}
	envelope.Payload[0] = 'X'
	envelope.Metadata["tenant"] = "changed"
	sql, args := p.Statement()
	if sql != "INSERT INTO \"public\".\"outbox_messages\" (id, topic, payload, payload_version, metadata, ordering_key, idempotency_key, attempts, available_at, created_at) VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10)" || len(args) != 10 || !bytes.Equal(args[2].([]byte), []byte("safe")) || args[4] != "{\"tenant\":\"tenant-a\"}" {
		t.Fatal("owned insert changed")
	}
	args[0] = "other"
	if args[3] != int32(1) {
		t.Fatal("prepared payload version must use restricted SQL scalar profile")
	}
	args[2].([]byte)[0] = 'Y'
	_, again := p.Statement()
	if again[0] != "effect-a" || string(again[2].([]byte)) != "safe" {
		t.Fatal("statement arguments alias plan")
	}
	if _, err = w.PrepareInsert(nil); err == nil {
		t.Fatal("empty plan accepted")
	}
	if _, err = (*Writer)(nil).PrepareInsert([]outbox.Envelope{envelope}); err == nil {
		t.Fatal("nil writer accepted")
	}
	if q, a := (PreparedInsert{}).Statement(); q != "" || a != nil {
		t.Fatal("zero prepared insert usable")
	}
}

func TestPreparedInsertAdmissionAndMultirowScalarShape(t *testing.T) {
	w, err := NewWriter(WriterConfig{MaxBatchSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	valid := outbox.Envelope{ID: "one", Topic: "identity.created.v1", Payload: []byte("safe"), PayloadVersion: 65535, CreatedAt: now, AvailableAt: now}
	for name, test := range map[string]struct {
		writer *Writer
		batch  []outbox.Envelope
		want   error
	}{
		"nil-writer":  {nil, []outbox.Envelope{valid}, ErrWriterRequired},
		"zero-writer": {&Writer{}, []outbox.Envelope{valid}, ErrWriterRequired},
		"empty":       {w, nil, ErrEmptyBatch},
		"oversize":    {w, []outbox.Envelope{valid, valid, valid}, ErrBatchTooLarge},
	} {
		t.Run(name, func(t *testing.T) {
			plan, err := test.writer.PrepareInsert(test.batch)
			query, args := plan.Statement()
			if !errors.Is(err, test.want) || query != "" || args != nil {
				t.Fatal("invalid preparation retained a statement", err)
			}
		})
	}
	for _, mutate := range []func(*outbox.Envelope){
		func(e *outbox.Envelope) { e.PayloadVersion = 0 },
		func(e *outbox.Envelope) { e.ID = strings.Repeat("a", maxIdentifierBytes+1) },
		func(e *outbox.Envelope) { e.Metadata = map[string]string{"invalid": "nul\x00value"} },
	} {
		invalid := valid
		mutate(&invalid)
		plan, err := w.PrepareInsert([]outbox.Envelope{valid, invalid})
		query, args := plan.Statement()
		if err == nil || query != "" || args != nil {
			t.Fatal("invalid later envelope left an executable prefix")
		}
	}
	second := valid
	second.ID = "two"
	plan, err := w.PrepareInsert([]outbox.Envelope{valid, second})
	if err != nil {
		t.Fatal(err)
	}
	query, args := plan.Statement()
	if len(args) != 20 || !strings.Contains(query, "$5::jsonb") || !strings.Contains(query, "$15::jsonb") || args[3] != int32(65535) || args[13] != int32(65535) || args[4] != "{}" || args[14] != "{}" {
		t.Fatal("multirow metadata or scalar version profile changed")
	}
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 20 {
				_, copy := plan.Statement()
				if string(copy[2].([]byte)) != "safe" || copy[0] != "one" {
					t.Error("concurrent reader observed foreign storage")
				}
				copy[2].([]byte)[0] = 'X'
				copy[0] = "foreign"
			}
		})
	}
	readers.Wait()
}
