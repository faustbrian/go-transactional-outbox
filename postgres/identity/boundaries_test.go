package identityoutbox

import (
	"context"
	"encoding/base64"
	"testing"

	identity "github.com/faustbrian/go-identity"
)

// This probes the persisted command binding, not public Work authority or
// cryptographic interoperability. Invalid command encodings must not reach SQL.
func FuzzLifecycleCommandAdmission(f *testing.F) {
	f.Add("")
	f.Add("invalid")
	f.Add(string(lifecycleFixture().CommandID))
	f.Fuzz(func(t *testing.T, command string) {
		if len(command) > 4096 {
			t.Skip()
		}
		c, err := NewContributor()
		if err != nil {
			t.Fatal(err)
		}
		r := lifecycleFixture()
		r.CommandID = identity.CommandID(command)
		queries, writes := 0, 0
		err = c.persist(context.Background(), func(context.Context, string, ...any) (int64, error) {
			writes++
			return 1, nil
		}, func(context.Context, string, ...any) (bool, error) {
			queries++
			return true, nil
		}, r)
		decoded, invalid := base64.RawURLEncoding.DecodeString(command)
		if invalid != nil || len(decoded) != 16 {
			if err == nil || queries != 0 || writes != 0 {
				t.Fatal("invalid command binding reached persistence")
			}
		} else if err == nil && (queries != 1 || writes != 1) {
			t.Fatal("accepted binding did not preserve one append boundary")
		}
	})
}

// BenchmarkLifecycleEnvelope measures versioned JSON and ordering-key framing
// for the fixed lossless lifecycle fixture. It excludes SQL, transactions,
// consumer delivery and concurrent contention; it is not end-to-end throughput.
func BenchmarkLifecycleEnvelope(b *testing.B) {
	r := lifecycleFixture()
	b.ReportAllocs()
	for b.Loop() {
		envelope, err := lifecycleEnvelope(r)
		if err != nil || envelope.ID != r.ID || len(envelope.Payload) == 0 {
			b.Fatal("lifecycle envelope unavailable")
		}
	}
}
