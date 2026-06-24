# Fix Distributed Benchmark Subscriptions

This plan addresses the remaining issue identified in the benchmark optimization report: **Gateways 4 and 5 are not receiving messages**.

## Root Causes Identified

1. **Nginx Upstream Misconfiguration:**
   The `nginx/nginx-benchmark.conf` file only lists `gateway1`, `gateway2`, and `gateway3` in the `rtmds_gateways` upstream block. Because gateways 4 and 5 are never receiving traffic, no clients ever connect to them, and thus the distributed router never subscribes to their Redis channels. They remain perfectly idle.
2. **Missing Reconciler Lifecycle Hook:**
   During the investigation, I discovered that the `DistributedRouter.StartReconciler()` method is never invoked in `internal/app/app.go`. If a Redis subscription network call were to fail during a client connect, the router would never retry, leaving the subscription permanently broken.

## Proposed Changes

### 1. Update Nginx Benchmark Config
We will update the Nginx configuration to include gateways 4 and 5. Since Nginx will fail to start if it cannot resolve these DNS names (and they only exist when the `5gateways` profile is used), we will configure Nginx to resolve them at runtime or safely ignore them if they are down.
Wait, Docker Compose networking resolves service names. If we start with 3 gateways, `gateway4` and `gateway5` won't be resolvable, and Nginx will crash on startup unless we use a `resolver` variable hack. 
Alternatively, we can create a dedicated `nginx-benchmark-5gw.conf` or just document that the 5-gateway benchmark requires uncommenting those lines. 
*Actually, the simplest robust fix for Docker Compose is to always start all 5 gateways but only send traffic to 3 during the 3-gateway test, or use two different Nginx upstream blocks. I will add them directly and we can use the `5gateways` profile whenever we run the benchmark.*

#### [MODIFY] [nginx-benchmark.conf](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/nginx/nginx-benchmark.conf)
- Add `server gateway4:9094 max_fails=2 fail_timeout=10s;`
- Add `server gateway5:9095 max_fails=2 fail_timeout=10s;`

### 2. Wire the Router Reconciler
We will add the missing reconciler start hook in the application startup sequence to ensure resilient Redis subscriptions.

#### [MODIFY] [app.go](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/app/app.go)
- Add a new component registration for the "distributed-reconciler".
- Start the reconciler loop in a background goroutine and shut it down cleanly when the application stops.

## Verification Plan

1. Start the 5-gateway benchmark cluster.
2. Verify Nginx successfully routes traffic to `gateway4` and `gateway5`.
3. Verify that the CPU and Redis network I/O spread evenly across all 5 gateways instead of just the first 3.
