<#
.SYNOPSIS
    Compares two `go test -bench` output files and prints each shared
    benchmark's old/new ns/op and the percent delta.

.DESCRIPTION
    A small, single-purpose diff for spotting performance regressions
    between a saved baseline (e.g. benchmark/results/phase13-baseline.txt)
    and a later run. It does not install or shell out to `benchstat`;
    it parses the standard `go test -bench` text format directly, since
    adding a dependency for a one-line comparison is out of proportion to
    what this phase needs (see benchmark/README.md's rule against turning
    this into a large performance framework).

.PARAMETER Old
    Path to the baseline `go test -bench` output file.

.PARAMETER New
    Path to the new `go test -bench` output file.

.EXAMPLE
    ./benchmark/scripts/compare.ps1 -Old benchmark/results/phase13-baseline.txt -New benchmark/results/after-change.txt
#>
param(
    [Parameter(Mandatory = $true)][string]$Old,
    [Parameter(Mandatory = $true)][string]$New
)

function Read-BenchResults {
    param([string]$Path)
    $results = @{}
    if (-not (Test-Path $Path)) {
        throw "file not found: $Path"
    }
    foreach ($line in Get-Content $Path) {
        # A benchmark result line looks like:
        #   BenchmarkFoo/bar-16    12345    678.9 ns/op    ...
        if ($line -match '^(Benchmark\S+)\s+\d+\s+([\d.]+)\s+ns/op') {
            $name = $matches[1]
            $nsPerOp = [double]$matches[2]
            # A later line for the same name (e.g. a re-run) overwrites
            # the earlier one, so comparing a file against itself is a
            # harmless no-op rather than an error.
            $results[$name] = $nsPerOp
        }
    }
    return $results
}

$oldResults = Read-BenchResults -Path $Old
$newResults = Read-BenchResults -Path $New

$names = @($oldResults.Keys) + @($newResults.Keys) | Select-Object -Unique | Sort-Object

Write-Host ("{0,-55} {1,15} {2,15} {3,10}" -f "benchmark", "old ns/op", "new ns/op", "delta %")
Write-Host ("-" * 100)

foreach ($name in $names) {
    if (-not $oldResults.ContainsKey($name)) {
        Write-Host ("{0,-55} {1,15} {2,15} {3,10}" -f $name, "(missing)", $newResults[$name], "new")
        continue
    }
    if (-not $newResults.ContainsKey($name)) {
        Write-Host ("{0,-55} {1,15} {2,15} {3,10}" -f $name, $oldResults[$name], "(missing)", "removed")
        continue
    }
    $o = $oldResults[$name]
    $n = $newResults[$name]
    $delta = if ($o -ne 0) { (($n - $o) / $o) * 100 } else { 0 }
    $inv = [System.Globalization.CultureInfo]::InvariantCulture
    $deltaStr = "{0:+0.0;-0.0;0.0}%" -f $delta
    $oStr = $o.ToString("N1", $inv)
    $nStr = $n.ToString("N1", $inv)
    Write-Host ("{0,-55} {1,15} {2,15} {3,10}" -f $name, $oStr, $nStr, $deltaStr)
}
