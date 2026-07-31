# run_redis.ps1
# Automates the Redis Pub/Sub benchmarking up to 1M msgs/sec

$ErrorActionPreference = "Stop"

Write-Host "Building Redis Benchmark..."
go build -o redis_bench.exe ./main.go

$Rates = @(1000, 10000, 50000, 100000, 250000, 500000, 1000000)
$Duration = "15s"
$Redis = "localhost:6379"

Write-Host "Starting Isolated Redis Benchmark..."
Write-Host "---------------------------------"

foreach ($rate in $Rates) {
    Write-Host "Running Redis Benchmark at $rate msg/s..."
    
    .\redis_bench.exe -rate $rate -duration $Duration -redis $Redis
    
    Write-Host "Finished $rate msg/s run.`n"
    Start-Sleep -Seconds 3
}

Write-Host "Generating combined scaling charts..."
python plot.py

Write-Host "All benchmarks complete! Scaling charts are saved as PNG files."
