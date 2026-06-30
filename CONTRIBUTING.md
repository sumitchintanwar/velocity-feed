# Contributing to RTMDS

First, thank you for your interest in contributing to the Real-Time Market Data System (RTMDS)! Building a high-throughput, low-latency distributed system requires strict adherence to engineering rigor, and we welcome contributions that help improve the platform's stability, performance, and features.

## 1. Branching Strategy

We follow a simplified Git Flow model:
- `main` is the primary development branch. All feature branches branch off from `main`.
- `release/vX.Y.Z` branches are created from `main` for production deployments.
- Feature branches should be named `feature/<issue-number>-<brief-description>` (e.g., `feature/42-redis-reconnect`).
- Bugfix branches should be named `bugfix/<issue-number>-<brief-description>`.

## 2. Pull Request Workflow

1. **Fork & Branch:** Create your feature branch from the latest `main`.
2. **Develop & Test:** Write your code, ensuring it meets our coding standards and passes all tests.
3. **Commit Messages:** Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification.
   - Example: `feat(topicmanager): implement dynamic backpressure scaling`
   - Example: `fix(gateway): resolve race condition in websocket teardown`
4. **Open a PR:** Open a Pull Request against `main`. 
5. **CI Checks:** Ensure all GitHub Actions / CI pipelines pass (Linting, Unit Tests, Race Detector).
6. **Code Review:** Wait for a maintainer to review your code.

## 3. Code Review Expectations

To ensure PRs are merged smoothly, please ensure the following:

- **Performance First:** RTMDS is a latency-sensitive application. Avoid unnecessary heap allocations, global locks, and reflection. If introducing a new dependency or algorithm, provide benchmark results in your PR description.
- **Test Coverage:** All new features must include unit tests. Bug fixes must include a regression test that fails before the fix and passes after.
- **Documentation:** If your change modifies architecture, updates APIs, or introduces a new configuration variable, update the corresponding Markdown documentation in `docs/` and `api/openapi.yaml`.
- **Atomic Commits:** Keep your commits logical and atomic. Avoid monolithic PRs that refactor unrelated systems.

## 4. Development Environment

Please see our `docs/onboarding/ONBOARDING_GUIDE.md` for full instructions on setting up your local Go toolchain, Docker Compose stack, and test environment.

### Quick Commands
- `make build` - Compiles the project
- `make test` - Runs unit tests with the race detector
- `make lint` - Runs `golangci-lint`

## 5. Architectural Changes

If you plan to make a significant architectural change (e.g., changing the messaging broker, altering the threading model, or introducing a major new service):
1. Please open a GitHub Issue with the `enhancement` label first to discuss the proposal with maintainers.
2. We require an Architecture Decision Record (ADR) for major systemic changes.

Thank you for contributing!
