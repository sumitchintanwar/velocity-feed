# collect_profiles.ps1
# Automates Go Profiling of the RTMDS Gateway under load

$ErrorActionPreference = "Stop"

$PprofHost = "http://localhost:8080/debug/pprof"

Write-Host "Starting Profile Collection..."
Write-Host "=============================="

# Check if target is up
try {
    Invoke-WebRequest -Uri "$PprofHost/" -UseBasicParsing | Out-Null
} catch {
    Write-Error "Gateway is not running or pprof is not enabled at $PprofHost"
}

# 1. Goroutine Profile
Write-Host "Collecting Goroutine Profile..."
go tool pprof -raw -output goroutines.txt "$PprofHost/goroutine"

# 2. Heap Profile
Write-Host "Collecting Heap Profile..."
go tool pprof -raw -output heap.txt "$PprofHost/heap"

# 3. CPU Profile (30 seconds)
Write-Host "Collecting CPU Profile (Takes 30 seconds)..."
go tool pprof -raw -output cpu.txt "$PprofHost/profile?seconds=30"

# 4. Mutex Profile
Write-Host "Collecting Mutex Profile..."
go tool pprof -raw -output mutex.txt "$PprofHost/mutex"

# 5. Block Profile
Write-Host "Collecting Block Profile..."
go tool pprof -raw -output block.txt "$PprofHost/block"

Write-Host "=============================="
Write-Host "Profile collection complete! Raw files saved."
Write-Host "Run 'go tool pprof -http=:8081 <url>' to view interactively."
