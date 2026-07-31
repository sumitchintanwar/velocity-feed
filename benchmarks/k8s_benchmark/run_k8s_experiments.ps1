# run_k8s_experiments.ps1
# Automates Kubernetes Deployment Benchmarking Matrix
# Assumes 'kubectl' is configured and the cluster is currently active in the default namespace.

$ErrorActionPreference = "Stop"

$PublisherReplicas = @(1, 2, 4)
$GatewayReplicas = @(1, 2, 3, 5)

$TargetClients = 5000
$Duration = "20s"
$Rate = 10000

# We assume standard paths to the pre-built benchmark binaries
$ClientLoadExe = "..\client_load\client_load.exe"
$PublisherLoadExe = "..\publisher_load\publisher_load.exe"

# Retrieve active Node IP and Ports (assuming NodePort for local tests)
# Adjust these based on your Ingress/Service setup
$WsUrl = "ws://localhost:8080/ws" 
$MetricsUrl = "http://localhost:8080/metrics"
$RedisAddr = "localhost:6379"

Write-Host "Starting Kubernetes Benchmark Suite..."
Write-Host "======================================"

function Wait-For-Pods {
    param ($Deployment)
    Write-Host "Waiting for $Deployment pods to become Ready..."
    kubectl rollout status deployment/$Deployment --timeout=60s
}

function Record-Resource-Usage {
    param ($Label)
    Write-Host "--- Resource Usage: $Label ---"
    Write-Host "Nodes:"
    kubectl top nodes
    Write-Host "Pods:"
    kubectl top pods
    Write-Host "------------------------------"
}

# 1. HORIZONTAL SCALING MATRIX
foreach ($pubRep in $PublisherReplicas) {
    foreach ($gwRep in $GatewayReplicas) {
        Write-Host "`n>> Scaling Publisher to $pubRep | Gateway to $gwRep"
        
        kubectl scale deployment publisher --replicas=$pubRep
        kubectl scale deployment gateway --replicas=$gwRep
        
        Wait-For-Pods -Deployment "publisher"
        Wait-For-Pods -Deployment "gateway"
        
        # Give services a few seconds to discover each other via CoreDNS
        Start-Sleep -Seconds 5
        
        Write-Host "Running Benchmark against matrix..."
        
        # Start background publisher load
        $pubJob = Start-Process -FilePath $PublisherLoadExe -ArgumentList "-rate $Rate -duration $Duration -redis $RedisAddr -target $MetricsUrl" -PassThru -NoNewWindow
        
        # Start client load
        $clientJob = Start-Process -FilePath $ClientLoadExe -ArgumentList "-clients $TargetClients -duration $Duration -url $WsUrl -target $MetricsUrl" -PassThru -NoNewWindow
        
        # Capture K8s resources mid-test
        Start-Sleep -Seconds 10
        Record-Resource-Usage "Pub:$pubRep|Gw:$gwRep"
        
        Wait-Process -Id $pubJob.Id
        Wait-Process -Id $clientJob.Id
        
        # Output summary files will be saved in the CWD by the benchmark tools
        Write-Host "Matrix test complete."
    }
}

# 2. ROLLING DEPLOYMENT IMPACT TEST
Write-Host "`n>> Initiating Rolling Deployment Impact Test"
# Reset to a stable baseline
kubectl scale deployment publisher --replicas=2
kubectl scale deployment gateway --replicas=3
Wait-For-Pods -Deployment "gateway"

Write-Host "Starting baseline load for 30 seconds..."
$pubJob = Start-Process -FilePath $PublisherLoadExe -ArgumentList "-rate 10000 -duration 30s -redis $RedisAddr -target $MetricsUrl" -PassThru -NoNewWindow
$clientJob = Start-Process -FilePath $ClientLoadExe -ArgumentList "-clients 2000 -duration 30s -url $WsUrl -target $MetricsUrl" -PassThru -NoNewWindow

Start-Sleep -Seconds 5
Write-Host "Triggering Rolling Restart on Gateway!"
kubectl rollout restart deployment gateway

# Monitor rollout time
$rolloutStart = Get-Date
kubectl rollout status deployment/gateway
$rolloutEnd = Get-Date
$rolloutDuration = ($rolloutEnd - $rolloutStart).TotalSeconds

Write-Host "Rollout completed in $rolloutDuration seconds."

Wait-Process -Id $pubJob.Id
Wait-Process -Id $clientJob.Id

Write-Host "Kubernetes Benchmark Suite Completed successfully."
