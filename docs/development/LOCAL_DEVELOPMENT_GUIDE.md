# Local Development Guide

**Purpose:** To help new engineers quickly configure their machines for building, testing, and running the platform locally.
**Intended Audience:** New Hires, Software Engineers.
**Maintenance Strategy:** Update if new dependencies (e.g., a new database or caching layer) are added to the stack.

---

## 1. Prerequisites
- **Go 1.22+**
- **Docker Desktop** (with Compose plugin)
- **Make**

## 2. Bootstrapping the Environment
We use Docker Compose to orchestrate the entire supporting infrastructure (Redis, PostgreSQL, Prometheus, Jaeger) so you don't have to install them natively.

```bash
# Start all dependencies in the background
docker-compose up -d

# Verify services are healthy
docker ps
```

## 3. Running the Application
You can run the RTMDS server natively on your host machine to easily attach a debugger (like Delve) or iterate quickly without rebuilding Docker images.

```bash
# Build and run the server
go run cmd/server/main.go
```
The Gateway will be listening on `:8080` and the Admin API on `:8081`.

### 3.1 Injecting Synthetic Market Data
If you boot the system and connect to the WebSocket, you won't see any data until you inject it. You can manually push a synthetic tick into the Publisher using `curl`:

```bash
curl -X POST http://localhost:8080/publish \
     -H "Content-Type: application/json" \
     -d '{
           "type": "trade",
           "instrument": "AAPL",
           "price": 150.00,
           "volume": 10
         }'
```
Once injected, the Gateway will instantly broadcast this to your connected WebSocket clients.

## 4. Testing
All tests must pass locally before pushing code.

```bash
# Run unit tests
go test -short ./...

# Run race detector
go test -race ./...

# Execute the Chaos Engineering Suite
go run cmd/qa_chaos/main.go
```

## 5. Teardown
To cleanly shut down the local environment and wipe the Docker volumes (to clear the Postgres database):
```bash
docker-compose down -v
```
