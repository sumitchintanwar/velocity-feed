# Deployment Guide

**Purpose:** To detail the exact procedures for rolling the RTMDS platform into production environments.
**Intended Audience:** SREs, Platform Engineers.
**Maintenance Strategy:** Update immediately if the Kustomize manifests, Helm charts, or Docker Compose structures change.

---

## 1. Local Development (Docker Compose)
For local testing and QA, the entire stack (including Redis, Postgres, Prometheus, and Grafana) is orchestrated via Docker Compose.

**Execution:**
```bash
# Build the binary and launch the cluster
docker-compose up --build -d
```
The Gateway will be available at `ws://localhost:8080/ws`.

## 2. Production (Kubernetes)
The platform is deployed to production Kubernetes clusters using `kustomize`.

### 2.1 Pre-Deployment Checks
1. Ensure the PostgreSQL `StatefulSet` is healthy:
   ```bash
   kubectl get statefulset rtmds-postgres -n rtmds-prod -o jsonpath='{.status.readyReplicas}'
   ```
2. Ensure the Redis cluster reports `cluster_state:ok` via Sentinel.
3. Verify the targeted container image tag is present in the enterprise registry.

### 2.2 Rolling Update Execution
We utilize the `overlays/prod` configuration which enforces Horizontal Pod Autoscalers (HPA), Pod Disruption Budgets (PDB), and strict `SecurityContexts`.

```bash
# Apply the production overlay
kubectl apply -k k8s/overlays/prod

# Monitor the rollout status
kubectl rollout status deployment/rtmds-gateway -n rtmds-prod
kubectl rollout status deployment/rtmds-publisher -n rtmds-prod
```

### 2.3 Rollback Procedure
If the automated Readiness probes fail, or if Error Rate alerts fire immediately after deployment, execute an instant rollback:
```bash
kubectl rollout undo deployment/rtmds-gateway -n rtmds-prod
kubectl rollout undo deployment/rtmds-publisher -n rtmds-prod
```
