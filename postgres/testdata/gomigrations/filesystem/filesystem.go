// Package filesystem adapts trusted embedded outbox SQL for migration tests.
package filesystem

import (
	"context"
	migrations "github.com/faustbrian/go-migrations/v3"
	"io/fs"
)

// FS enforces the source budgets for trusted, in-memory migration fixtures.
type FS struct {
	Files fs.FS
}

func (filesystem FS) ReadDir(
	ctx context.Context,
	root string,
	limits migrations.SourceDirectoryLimits,
) ([]migrations.SourceEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := fs.ReadDir(filesystem.Files, root)
	if err != nil {
		return nil, err
	}
	if len(entries) > limits.MaxEntries {
		return nil, migrations.ErrSourceLimit
	}
	converted := make([]migrations.SourceEntry, 0, len(entries))
	totalNameBytes := 0
	for _, entry := range entries {
		name := entry.Name()
		if len(name) > limits.MaxNameBytes || len(name) > limits.MaxTotalNameBytes-totalNameBytes {
			return nil, migrations.ErrSourceLimit
		}
		totalNameBytes += len(name)
		converted = append(converted, migrations.SourceEntry{Name: name, Directory: entry.IsDir()})
	}

	return converted, ctx.Err()
}

func (filesystem FS) ReadFile(
	ctx context.Context,
	name string,
	maxBytes int,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	contents, err := fs.ReadFile(filesystem.Files, name)
	if err != nil {
		return nil, err
	}
	if len(contents) > maxBytes {
		return nil, migrations.ErrInvalidEncoding
	}

	return contents, ctx.Err()
}
