PYPROJECT := pyproject.toml

.PHONY: release release-minor release-major

release:
	$(eval CURRENT := $(shell grep '^version' $(PYPROJECT) | sed 's/.*"\(.*\)"/\1/'))
	$(eval PARTS := $(subst ., ,$(CURRENT)))
	$(eval MAJOR := $(word 1,$(PARTS)))
	$(eval MINOR := $(word 2,$(PARTS)))
	$(eval PATCH := $(word 3,$(PARTS)))
	$(eval NEW := $(MAJOR).$(MINOR).$(shell echo $$(($(PATCH)+1))))
	sed -i 's/^version = "$(CURRENT)"/version = "$(NEW)"/' $(PYPROJECT)
	@echo ""
	@echo "Bumped $(CURRENT) -> $(NEW)"
	@echo ""
	@echo "Run:"
	@echo "  git add $(PYPROJECT)"
	@echo "  git commit -m 'chore: release v$(NEW)'"
	@echo "  git tag v$(NEW)"
	@echo "  git push origin main --tags"

release-minor:
	$(eval CURRENT := $(shell grep '^version' $(PYPROJECT) | sed 's/.*"\(.*\)"/\1/'))
	$(eval PARTS := $(subst ., ,$(CURRENT)))
	$(eval MAJOR := $(word 1,$(PARTS)))
	$(eval MINOR := $(word 2,$(PARTS)))
	$(eval NEW := $(MAJOR).$(shell echo $$(($(MINOR)+1))).0)
	sed -i 's/^version = "$(CURRENT)"/version = "$(NEW)"/' $(PYPROJECT)
	@echo ""
	@echo "Bumped $(CURRENT) -> $(NEW)"
	@echo ""
	@echo "Run:"
	@echo "  git add $(PYPROJECT)"
	@echo "  git commit -m 'chore: release v$(NEW)'"
	@echo "  git tag v$(NEW)"
	@echo "  git push origin main --tags"

release-major:
	$(eval CURRENT := $(shell grep '^version' $(PYPROJECT) | sed 's/.*"\(.*\)"/\1/'))
	$(eval PARTS := $(subst ., ,$(CURRENT)))
	$(eval MAJOR := $(word 1,$(PARTS)))
	$(eval NEW := $(shell echo $$(($(MAJOR)+1))).0.0)
	sed -i 's/^version = "$(CURRENT)"/version = "$(NEW)"/' $(PYPROJECT)
	@echo ""
	@echo "Bumped $(CURRENT) -> $(NEW)"
	@echo ""
	@echo "Run:"
	@echo "  git add $(PYPROJECT)"
	@echo "  git commit -m 'chore: release v$(NEW)'"
	@echo "  git tag v$(NEW)"
	@echo "  git push origin main --tags"
