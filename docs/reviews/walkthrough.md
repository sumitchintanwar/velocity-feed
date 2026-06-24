# Distributed Benchmark Fixes Walkthrough

I have implemented the fixes identified in the implementation plan to resolve the issue where Gateways 4 and 5 were completely idle during the 5-gateway distributed benchmark.

## What Was Changed

### 1. Nginx Upstream Config
Added `gateway4` and `gateway5` to the `rtmds_gateways` upstream block in [nginx-benchmark.conf](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/nginx/nginx-benchmark.conf). This ensures that Nginx actually routes WebSocket connections to these nodes, allowing their distributed routers to dynamically subscribe to Redis.

### 2. Docker Compose Profiles
Removed the `profiles: [5gateways]` constraint from `gateway4` and `gateway5` in [docker-compose.benchmark.yml](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/docker-compose.benchmark.yml). 
Because open-source Nginx will crash on startup if an upstream server's DNS name does not exist, gateways 4 and 5 must always be running to satisfy the updated upstream config. The benchmark now defaults to a 5-gateway configuration whenever it is run.

### 3. Distributed Router Reconciler
Wired the `DistributedRouter.StartReconciler()` lifecycle hook in [app.go](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/app/app.go#L432). I added the `start` function to the `distributed-router-background` component. 

```go
start: func(ctx context.Context) error {
    router.StartReconciler(ctx, 5*time.Second)
    a.log.Info().Msg("distributed-router: reconciler started")
    return nil
},
```

If a Redis subscription attempt fails due to a transient network error, the reconciler loop will now reliably retry it every 5 seconds.

## Validation
The application successfully compiles (`go build -o bin/rtmds ./cmd/server`) with the updated lifecycle component.

> [!TIP]
> The next time you run `docker compose -f docker-compose.benchmark.yml up --build -d`, the load will be automatically distributed across all 5 gateways, which should yield true scaling efficiency numbers for the stress test!
