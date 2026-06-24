# Running Tests in WSL

This project can be developed and tested from WSL 2 on Windows. Since Go may not be installed natively inside the WSL distro, this guide covers how to use the Windows-side Go installation from within WSL.

---

## Prerequisites

- **Go installed on Windows** (download from https://go.dev/dl/)
- **WSL 2** with a Linux distribution (Ubuntu, Debian, etc.)
- **Project cloned** inside the WSL filesystem or on a mounted Windows drive (`/mnt/c/...`, `/mnt/e/...`, etc.)

---

## Option 1: Use the Windows Go Binary from WSL (Recommended)

WSL 2 can execute Windows `.exe` binaries directly. If Go is installed on Windows, its binary is typically located at:

```text
/mnt/c/Program Files/Go/bin/go.exe
```

### Verify the installation

```bash
"/mnt/c/Program Files/Go/bin/go.exe" version
```

Expected output:

```text
go version go1.26.0 windows/amd64
```

### Run all tests

```bash
"/mnt/c/Program Files/Go/bin/go.exe" test -race -count=1 ./...
```

- `-race` — enables the race detector for concurrent safety checks
- `-count=1` — disables test caching so every run is fresh

### Run tests with coverage

```bash
"/mnt/c/Program Files/Go/bin/go.exe" test -race -count=1 -coverprofile=coverage.out -covermode=atomic ./...
"/mnt/c/Program Files/Go/bin/go.exe" tool cover -func=coverage.out | tail -1
```

### Run only unit tests

```bash
"/mnt/c/Program Files/Go/bin/go.exe" test -race -count=1 ./internal/...
```

### Run only integration tests

```bash
"/mnt/c/Program Files/Go/bin/go.exe" test -race -count=1 -timeout=30s ./test/integration/...
```

### Run benchmarks

```bash
"/mnt/c/Program Files/Go/bin/go.exe" test -bench=. -benchmem ./...
```

### Tidy modules

```bash
"/mnt/c/Program Files/Go/bin/go.exe" mod tidy
"/mnt/c/Program Files/Go/bin/go.exe" mod verify
```

### Build the project

```bash
"/mnt/c/Program Files/Go/bin/go.exe" build ./...
```

---

## Option 2: Add Go to the WSL PATH

If you prefer not to type the full path every time, add an alias to your shell profile (`~/.bashrc` or `~/.zshrc`):

```bash
echo 'alias go="/mnt/c/Program Files/Go/bin/go.exe"' >> ~/.bashrc
source ~/.bashrc
```

After this, you can use `go` directly:

```bash
go test -race -count=1 ./...
```

> **Note:** This runs the Windows binary. File paths passed to Go will be Windows-style paths internally. This works correctly for building and testing but may behave differently for tools that expect Linux-native paths (e.g., some linters).

---

## Option 3: Install Go Natively in WSL

For a fully native experience, install Go inside the WSL distro:

```bash
wget https://go.dev/dl/go1.26.4.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.26.4.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

Add to `~/.bashrc` for persistence:

```bash
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
```

Then run tests normally:

```bash
go test -race -count=1 ./...
```

---

## Option 4: Run Tests via Docker

If neither Go on Windows nor native Go in WSL is available, you can run tests using the official Go Docker image:

```bash
docker run --rm \
  -v "$(pwd):/src" \
  -w /src \
  golang:1.26-alpine \
  sh -c "apk add --no-cache git ca-certificates tzdata && go test -race -count=1 ./..."
```

> **Note:** Docker Desktop WSL integration must be enabled. Check Docker Desktop settings > Resources > WSL Integration.

---

## Project Test Structure

```text
internal/
  workerpool/       — Worker pool concurrency tests (10 tests)
  topicmanager/      — Topic manager pub/sub tests (20 tests)
  websocket/         — WebSocket gateway tests (15 tests)
  pubsub/            — In-memory pub/sub bus tests (15 tests)
  marketdata/simulator/ — Market data simulator tests (16 tests)
  feed/              — Feed pipeline tests (5 tests)
  transport/         — HTTP transport benchmarks (6 benchmarks)
  bench/             — Backpressure and e2e benchmarks (13 benchmarks)
test/
  integration/       — End-to-end WebSocket integration tests (2 tests)
```

### Makefile targets

| Command | Description |
|---------|-------------|
| `make test` | All tests with race detector and coverage |
| `make test-unit` | Unit tests only |
| `make test-integration` | Integration tests with 30s timeout |
| `make bench` | All benchmarks with memory stats |
| `make lint` | Static analysis (requires golangci-lint) |
| `make vet` | Go vet checks |
| `make tidy` | Tidy and verify modules |

When using the Windows Go binary from WSL, replace `make test` with the direct `go test` command shown above, or invoke Make through WSL if `make` is installed:

```bash
# If make is installed in WSL
make test

# Or use the Go binary directly
"/mnt/c/Program Files/Go/bin/go.exe" test -race -count=1 ./...
```

---

## Troubleshooting

### "go: command not found"

Go is not on your WSL `PATH`. Use the full path to the Windows binary or install Go natively in WSL (see Option 2 or Option 3).

### Slow test execution on `/mnt/` drives

Running tests from a Windows-mounted path (`/mnt/e/...`) is significantly slower than running from the native WSL filesystem (`~/...`) due to filesystem translation overhead. For faster runs, clone the project inside WSL:

```bash
cd ~
git clone <repo-url> rtmds
cd rtmds
go test -race -count=1 ./...
```

### Permission denied

Ensure the Go binary is executable. Windows `.exe` files are executable by default in WSL, but if you encounter issues:

```bash
chmod +x "/mnt/c/Program Files/Go/bin/go.exe"
```

### Module download fails with TLS errors

If `go mod tidy` or `go mod download` fails with certificate errors, ensure CA certificates are available:

```bash
# For Docker-based tests
apk add --no-cache ca-certificates

# For native WSL Go
sudo apt install ca-certificates
```
