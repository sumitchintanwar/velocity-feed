# serve_flamegraph.ps1
# Automates opening the local pprof Web UI (which contains the flamegraph)

param (
    [string]$ProfileType = "profile" # cpu (profile), heap, mutex, block
)

$PprofHost = "http://localhost:8080/debug/pprof"

Write-Host "Downloading $ProfileType profile..."
$tempFile = "temp_$ProfileType.pb.gz"

Invoke-WebRequest -Uri "$PprofHost/$ProfileType" -OutFile $tempFile

Write-Host "Launching go tool pprof web server on port 8081..."
Write-Host "Navigate to http://localhost:8081/ui/flamegraph to see the Flamegraph."
Write-Host "Press Ctrl+C to stop."

go tool pprof -http=:8081 $tempFile

Remove-Item $tempFile
