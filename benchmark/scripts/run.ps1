<#
.SYNOPSIS
    Runs ForgeDB's full Phase 13 benchmark suite and saves a timestamped,
    environment-annotated result file under benchmark/results/.

.DESCRIPTION
    Records the Git revision and Go version, then runs every benchmark
    package with flags chosen per-package (see benchmark/README.md's
    "-benchtime=Nx caveat" section for why internal/raft, internal/dbnode,
    and benchmark/endtoend need an explicit bounded iteration count rather
    than a time budget: raft.FilePersister rewrites the entire Raft log on
    every Propose, so a time-based run lets the log -- and each
    iteration's cost -- grow without bound).

.PARAMETER OutFile
    Where to save the result. Defaults to a timestamped file under
    benchmark/results/ so repeated runs never overwrite each other; pass
    an explicit path (e.g. benchmark/results/phase13-baseline.txt) to
    create or refresh a named baseline for scripts/compare.ps1.

.EXAMPLE
    ./benchmark/scripts/run.ps1
.EXAMPLE
    ./benchmark/scripts/run.ps1 -OutFile benchmark/results/phase13-baseline.txt
#>
param(
    [string]$OutFile = "benchmark/results/run-$(Get-Date -Format 'yyyyMMdd-HHmmss').txt"
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "../..")
Set-Location $repoRoot

$outDir = Split-Path $OutFile -Parent
if ($outDir -and -not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
}

"go version:    $(go version)" | Out-File -FilePath $OutFile -Encoding utf8
"git revision:  $(git rev-parse HEAD)" | Out-File -FilePath $OutFile -Append -Encoding utf8
"git status:    $(if ((git status --porcelain) -eq $null) { 'clean' } else { 'dirty (uncommitted changes present)' })" | Out-File -FilePath $OutFile -Append -Encoding utf8
"run started:   $(Get-Date -Format o)" | Out-File -FilePath $OutFile -Append -Encoding utf8
"---" | Out-File -FilePath $OutFile -Append -Encoding utf8

# Each entry: (package path, extra go test flags beyond -run '^$' -bench=. -benchmem).
# See benchmark/README.md for why raft/dbnode/endtoend use an explicit
# -benchtime=Nx instead of a time budget.
$packages = @(
    @{ Path = "./internal/storage/memtable/..."; BenchTime = "500ms" },
    @{ Path = "./internal/storage/wal/...";       BenchTime = "500ms" },
    @{ Path = "./internal/storage/...";           BenchTime = "3x" },
    @{ Path = "./internal/storage/sstable/...";   BenchTime = "500ms" },
    @{ Path = "./internal/storage/manifest/...";  BenchTime = "500ms" },
    @{ Path = "./internal/storage/compaction/..."; BenchTime = "5x" },
    @{ Path = "./internal/statemachine/...";      BenchTime = "500ms" },
    @{ Path = "./internal/raft/...";              BenchTime = "50x" },
    @{ Path = "./internal/dbnode/...";            BenchTime = "20x" },
    @{ Path = "./benchmark/endtoend/...";         BenchTime = "10x" }
)

foreach ($pkg in $packages) {
    Write-Host "Running benchmarks: $($pkg.Path) (-benchtime=$($pkg.BenchTime))"
    "" | Out-File -FilePath $OutFile -Append -Encoding utf8
    go test $pkg.Path -run '^$' -bench=. -benchtime=$($pkg.BenchTime) -benchmem 2>&1 |
        Out-File -FilePath $OutFile -Append -Encoding utf8
}

Write-Host "Results saved to $OutFile"
