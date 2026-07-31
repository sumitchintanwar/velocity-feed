# run_all.ps1
# Automates the execution of the Benchmark suite across varying load profiles.

$ErrorActionPreference = "Stop"

Write-Host "Building Publisher Load Generator..."
go build -o publisher_load.exe ./publisher_load/main.go
Write-Host "Building Client Load Generator..."
go build -o client_load.exe ./client_load/main.go

$ClientCounts = @(100, 500, 1000, 2000, 5000, 10000)
$Duration = "10s"
$Rate = 10000
$Redis = "localhost:6379"
$WsUrl = "ws://localhost:8080/ws"
$MetricsUrl = "http://localhost:8080/metrics"

Write-Host "Starting RTMDS Benchmark Suite..."
Write-Host "---------------------------------"

# Run Publisher Benchmark (Baseline)
Write-Host "Running Publisher Benchmark (Rate: $Rate msg/s)..."
.\publisher_load.exe -rate $Rate -duration $Duration -redis $Redis -target $MetricsUrl
Write-Host "Publisher Benchmark Complete.`n"

# Run Client Benchmarks
foreach ($count in $ClientCounts) {
    Write-Host "Running Client Benchmark for $count clients..."
    
    # Start publisher in the background to generate events for the clients
    $pubJob = Start-Process -FilePath ".\publisher_load.exe" -ArgumentList "-rate $Rate -duration 15s -redis $Redis -target $MetricsUrl" -PassThru -NoNewWindow
    
    # Run clients
    .\client_load.exe -clients $count -duration $Duration -url $WsUrl -target $MetricsUrl
    
    # Wait for publisher to finish
    Wait-Process -Id $pubJob.Id
    
    Write-Host "Finished $count clients run.`n"
    
    # Sleep to allow target service to recover
    Start-Sleep -Seconds 5
}

Write-Host "All benchmarks complete! Results saved in CSV, JSON, and Markdown formats."
