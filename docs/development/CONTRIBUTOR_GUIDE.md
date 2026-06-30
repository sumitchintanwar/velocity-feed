# Contributor Guide

**Purpose:** To establish clear expectations for engineers contributing code to the platform.
**Intended Audience:** Software Engineers, External Contributors.
**Maintenance Strategy:** Update if branching strategies or CI/CD pipeline structures change.

---

## 1. Branching Strategy
We follow a strict **Trunk-Based Development** model.
- **Main Branch:** `main` is always releasable.
- **Feature Branches:** Prefix your branches with `feature/`, `bugfix/`, or `hotfix/` followed by the Jira ticket number (e.g., `feature/RTMDS-102-add-snapshot-api`).
- **Lifespan:** Branches should not live longer than 48 hours. Use feature flags for incomplete work.

## 2. Pull Request (PR) Expectations
Every PR must pass the following CI gates before it can be merged:
1. `go fmt` and `golangci-lint`.
2. Unit tests (`go test -short`).
3. Integration tests (`go test -tags=integration`).
4. Chaos Engineering QA Validation (`go run cmd/qa_chaos/main.go`).

## 3. Commit Message Convention
We adhere to Conventional Commits.
- `feat: add snapshot REST endpoint`
- `fix: resolve race condition in websocket broadcaster`
- `docs: update operational runbooks`
- `refactor: extract prometheus logic into middleware`

## 4. Code Review
- Require **2 approvals** from core maintainers.
- Reviewers will look specifically for concurrency safety (lack of race conditions) and memory efficiency (zero-allocation hot paths).
