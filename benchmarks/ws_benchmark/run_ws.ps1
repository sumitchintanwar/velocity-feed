# run_ws.ps1
# Automates the WebSocket benchmark

$ErrorActionPreference = "Stop"

Write-Host "Building WS Benchmark..."
go build -o ws_bench.exe ./main.go

$ClientCounts = @(100, 500, 1000, 2000, 5000, 10000)
$Duration = "15s"
$WsUrl = "ws://localhost:8080/ws"

Write-Host "Starting Dedicated WS Benchmark..."
Write-Host "---------------------------------"

foreach ($count in $ClientCounts) {
    Write-Host "Running WS Benchmark for $count clients..."
    
    .\ws_bench.exe -clients $count -duration $Duration -url $WsUrl -chaos 0.05
    
    Write-Host "Generating charts for $count clients..."
    python plot.py ws_results_$count.json
    
    Write-Host "Finished $count clients run.`n"
    Start-Sleep -Seconds 3
}

Write-Host "All benchmarks complete! Charts are saved as PNG files."
