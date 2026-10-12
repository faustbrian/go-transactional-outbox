GOLIB ?= golib

.PHONY: source-docs-check check check-full ci cohesion inventory repository-check specification-check specification-check-online recovery migration-integration

check: source-docs-check
	./scripts/check-docs.sh --local
	$(GOLIB) check --local --all

check-full: source-docs-check
	$(GOLIB) check --all

ci: source-docs-check
	$(GOLIB) repository check
	./scripts/check-docs.sh --local
	$(GOLIB) check --local --all

specification-check:
	$(GOLIB) specification check

specification-check-online:
	$(GOLIB) specification check --online

recovery:
	./scripts/run-recovery-exercises.sh

migration-integration:
	./scripts/check-go-migrations.sh

cohesion:
	$(GOLIB) cohesion check

inventory repository-check:
	$(GOLIB) repository check

source-docs-check:
	$(GOLIB) source-docs check
