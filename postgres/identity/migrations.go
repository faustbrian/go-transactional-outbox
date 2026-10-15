package identityoutbox

import (
	"embed"
	"io/fs"
)

//go:embed migrations/*.sql
var files embed.FS

// Migrations returns additive adapter migrations, rooted at filenames, for a
// separate migration ledger/namespace from the root outbox migrations. Install
// all root outbox migrations first. Retained command/index/digest bindings have
// no payload foreign key and survive generic outbox pruning. Down is unsupported:
// dropping this evidence permits replay. Application/relay roles must not own
// the tables or hold DDL, TRUNCATE or trigger-disable privileges.
func Migrations() fs.FS { source, _ := fs.Sub(files, "migrations"); return source }
