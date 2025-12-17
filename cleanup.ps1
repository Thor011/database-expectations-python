# Cleanup script for database-expectations-python project

Write-Host "======================================================================"
Write-Host "DATABASE EXPECTATIONS - CLEANUP SCRIPT"
Write-Host "======================================================================"
Write-Host ""

`$cleaned = 0

# Clean database files
Write-Host "Cleaning database files..." -ForegroundColor Yellow
`$dbFiles = Get-ChildItem -Path . -Filter "*.db" -File -ErrorAction SilentlyContinue
if (`$dbFiles) {
    `$dbFiles | Remove-Item -Force
    Write-Host "  [OK] Removed `$(`$dbFiles.Count) database file(s)" -ForegroundColor Green
    `$cleaned += `$dbFiles.Count
} else {
    Write-Host "  [SKIP] No database files to clean" -ForegroundColor Gray
}

# Clean coverage data
Write-Host "Cleaning coverage data..." -ForegroundColor Yellow
if (Test-Path ".coverage") {
    Remove-Item ".coverage" -Force
    Write-Host "  [OK] Removed .coverage file" -ForegroundColor Green
    `$cleaned++
} else {
    Write-Host "  [SKIP] No coverage data to clean" -ForegroundColor Gray
}

# Clean Great Expectations directory
Write-Host "Cleaning Great Expectations directory..." -ForegroundColor Yellow
if (Test-Path "gx") {
    Remove-Item "gx" -Recurse -Force
    Write-Host "  [OK] Removed gx/ directory" -ForegroundColor Green
    `$cleaned++
} else {
    Write-Host "  [SKIP] No gx/ directory to clean" -ForegroundColor Gray
}

# Clean pytest cache
Write-Host "Cleaning pytest cache..." -ForegroundColor Yellow
if (Test-Path ".pytest_cache") {
    Remove-Item ".pytest_cache" -Recurse -Force
    Write-Host "  [OK] Removed .pytest_cache/ directory" -ForegroundColor Green
    `$cleaned++
} else {
    Write-Host "  [SKIP] No pytest cache to clean" -ForegroundColor Gray
}

# Clean Python cache
Write-Host "Cleaning Python cache files..." -ForegroundColor Yellow
`$pycache = Get-ChildItem -Path . -Filter "__pycache__" -Directory -Recurse -ErrorAction SilentlyContinue
if (`$pycache) {
    `$pycache | Remove-Item -Recurse -Force
    Write-Host "  [OK] Removed `$(`$pycache.Count) __pycache__ directories" -ForegroundColor Green
    `$cleaned += `$pycache.Count
} else {
    Write-Host "  [SKIP] No __pycache__ directories to clean" -ForegroundColor Gray
}

# Clean .pyc files
`$pycFiles = Get-ChildItem -Path . -Filter "*.pyc" -File -Recurse -ErrorAction SilentlyContinue
if (`$pycFiles) {
    `$pycFiles | Remove-Item -Force
    Write-Host "  [OK] Removed `$(`$pycFiles.Count) .pyc file(s)" -ForegroundColor Green
    `$cleaned += `$pycFiles.Count
} else {
    Write-Host "  [SKIP] No .pyc files to clean" -ForegroundColor Gray
}

Write-Host ""
Write-Host "======================================================================"
if (`$cleaned -eq 0) {
    Write-Host "[SUCCESS] Project already clean - no files removed" -ForegroundColor Green
} else {
    Write-Host "[SUCCESS] Cleanup complete - removed `$cleaned item(s)" -ForegroundColor Green
}
Write-Host "======================================================================"
