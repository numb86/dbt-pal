.PHONY: publish

publish:
	@# Check current branch
	@CURRENT_BRANCH=$$(git rev-parse --abbrev-ref HEAD); \
	if [ "$$CURRENT_BRANCH" != "main" ]; then \
		echo "Error: must be on 'main' branch (currently on '$$CURRENT_BRANCH')" >&2; \
		exit 1; \
	fi
	@# Check working tree is clean
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "Error: working tree is not clean. Commit or stash changes first." >&2; \
		exit 1; \
	fi
	@# Check required tools are installed
	@python3 -c "import build" 2>/dev/null || { echo "Error: 'build' is not installed. Run: pip install build" >&2; exit 1; }
	@command -v twine >/dev/null 2>&1 || { echo "Error: 'twine' is not installed. Run: pip install twine" >&2; exit 1; }
	rm -rf dist/
	python3 -m build
	twine upload dist/*
