<# 
New-FakeContacts.ps1
Generates CSV files with unique "Number", "First Name", "Last Name", "Create Date" rows.

Features
- Unique names & numbers across runs (persisted in generator_state.json)
- Registry CSV (generator_registry.csv) with SHA-256 hash per record
- Random dates within a configurable window
- Deterministic mode via -Seed
- Sensible fallbacks when unique name pool is exhausted

Columns
  Number        VARCHAR(20)
  First Name    VARCHAR(50)
  Last Name     VARCHAR(50)
  Create Date   VARCHAR(50)

Example
  .\New-FakeContacts.ps1 -OutputDir .\out -FileCount 3 -RecordsPerFile 50 -Seed 42
  .\New-FakeContacts.ps1 -OutputDir .\out -FileCount 2 -RecordsPerFile 100 -MaxAgeDays 30

Tip
  Use generated CSVs with your Import-TestCsv.ps1 pipeline.


Quick starts

Generate 3 files, each with 50 records, to .\out:

.\New-FakeContacts.ps1 -OutputDir .\out -FileCount 3 -RecordsPerFile 50


Deterministic/randomness (reproducible):

.\New-FakeContacts.ps1 -OutputDir .\out -FileCount 2 -RecordsPerFile 25 -Seed 1337


Bigger batch, wider date window:

.\New-FakeContacts.ps1 -OutputDir .\out -FileCount 5 -RecordsPerFile 200 -MaxAgeDays 90


Reset global uniqueness (start fresh):

.\New-FakeContacts.ps1 -ResetState

#>

[CmdletBinding()]
param(
  [string]$OutputDir      = $(if ($PSScriptRoot) { Join-Path $PSScriptRoot 'out' } else { Join-Path (Split-Path -Parent $PSCommandPath) 'out' }),
  [int]$FileCount         = 1,
  [int]$RecordsPerFile    = 50,
  [int]$StartNumber       = 10001,   # first Number if no prior state exists
  [int]$MaxAgeDays        = 45,      # Create Date within last N days
  [nullable[int]]$Seed    = $null,   # deterministic randomness if set
  [string]$FilePrefix     = 'contacts',

  # State & registry
  [string]$StatePath      = $(if ($PSScriptRoot) { Join-Path $PSScriptRoot 'generator_state.json' } else { Join-Path (Split-Path -Parent $PSCommandPath) 'generator_state.json' }),
  [string]$RegistryPath   = $(if ($PSScriptRoot) { Join-Path $PSScriptRoot 'generator_registry.csv' } else { Join-Path (Split-Path -Parent $PSCommandPath) 'generator_registry.csv' }),
  [switch]$ResetState     # discard previous uniqueness state
)

# ---------------- Helpers ----------------
function Ensure-Directory {
  param([Parameter(Mandatory)][string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

function Load-State {
  param([string]$Path, [int]$StartNumber)
  if ($ResetState -and (Test-Path -LiteralPath $Path)) { Remove-Item -LiteralPath $Path -Force }
  if (Test-Path -LiteralPath $Path) {
    try {
      return Get-Content -Raw -LiteralPath $Path | ConvertFrom-Json
    } catch {
      Write-Warning "State file unreadable; starting fresh. $($_.Exception.Message)"
    }
  }
  # New state object
  [pscustomobject]@{
    UsedNames   = @{}   # dictionary of "First|Last" => 1
    UsedNumbers = @{}   # dictionary of "Number" => 1
    LastNumber  = $StartNumber - 1
  }
}

function Save-State {
  param([Parameter(Mandatory)]$State, [Parameter(Mandatory)][string]$Path)
  $State | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $Path -Encoding UTF8
}

function New-Random {
  param([nullable[int]]$Seed)
  if ($Seed -ne $null) { return [System.Random]::new($Seed) }
  else { return [System.Random]::new() }
}

function Get-Sha256Hex {
  param([Parameter(Mandatory)][string]$Text)
  $sha = [System.Security.Cryptography.SHA256]::Create()
  try {
    $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
    $hash  = $sha.ComputeHash($bytes)
    -join ($hash | ForEach-Object { $_.ToString('x2') })
  } finally { $sha.Dispose() }
}

function Get-Timestamp { Get-Date -Format 'yyyyMMdd_HHmmss' }

# ---------------- Name Pools ----------------
# (Add/modify to suit your data variety; 50x50 gives 2500 combos)
$FirstNames = @(
  "Alex","Jordan","Taylor","Morgan","Casey","Riley","Avery","Cameron","Drew","Harper",
  "Reese","Skyler","Parker","Quinn","Rowan","Sage","Shawn","Sidney","Logan","Blair",
  "Elliot","Jamie","Kendall","Lane","Milan","Nico","Oakley","Payton","Reagan","Shiloh",
  "Sydney","Tatum","Tristan","Wesley","Zion","Emery","Finley","Hayden","Jules","Kai",
  "Marley","Noel","Phoenix","Remy","Sasha","Toby","Wren","Adrian","Bailey","Dakota"
)
$LastNames = @(
  "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
  "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin",
  "Lee","Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson",
  "Walker","Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores",
  "Green","Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell","Carter","Roberts"
)

function Get-UniqueName {
  param(
    [Parameter(Mandatory)]$State,
    [Parameter(Mandatory)][System.Random]$Rnd
  )
  # Try random combos from pools
  $maxAttempts = 2000
  for ($i=0; $i -lt $maxAttempts; $i++) {
    $fn = $FirstNames[$Rnd.Next(0,$FirstNames.Count)]
    $ln = $LastNames[$Rnd.Next(0,$LastNames.Count)]
    $k = "$fn|$ln"
    if (-not $State.UsedNames.ContainsKey($k)) {
      $State.UsedNames[$k] = 1
      return @{ 'First Name'=$fn; 'Last Name'=$ln }
    }
  }
  # Fallback: synthesize a unique last name by suffixing a number
  # (Guarantees uniqueness even after exhausting base pool)
  $fnF = $FirstNames[$Rnd.Next(0,$FirstNames.Count)]
  $lnF = $LastNames[$Rnd.Next(0,$LastNames.Count)]
  for ($n=1; $n -le 100000; $n++) {
    $ln2 = "$lnF$n"
    $k2 = "$fnF|$ln2"
    if (-not $State.UsedNames.ContainsKey($k2)) {
      $State.UsedNames[$k2] = 1
      return @{ 'First Name'=$fnF; 'Last Name'=$ln2 }
    }
  }
  throw "Unable to generate a unique name after exhausting fallbacks."
}

function Get-NextNumber {
  param([Parameter(Mandatory)]$State)
  $State.LastNumber = [int]$State.LastNumber + 1
  $num = $State.LastNumber.ToString()
  $State.UsedNumbers[$num] = 1
  return $num
}

function Get-RandomCreateDate {
  param([Parameter(Mandatory)][System.Random]$Rnd, [int]$MaxAgeDays)
  $days  = $Rnd.Next(0, [Math]::Max(1,$MaxAgeDays+1))
  $h     = $Rnd.Next(0,24)
  $m     = $Rnd.Next(0,60)
  $s     = $Rnd.Next(0,60)
  # ISO-like string "YYYY-MM-DD HH:MM:SS"
  (Get-Date).AddDays(-$days).Date.AddHours($h).AddMinutes($m).AddSeconds($s).ToString('yyyy-MM-dd HH:mm:ss')
}

function Ensure-RegistryHeader {
  param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) {
    "KeyHash,Number,First Name,Last Name,Create Date,FileName" | Set-Content -LiteralPath $Path -Encoding UTF8
  }
}

# ---------------- Main ----------------
try {
  Ensure-Directory -Path $OutputDir
  $state = Load-State -Path $StatePath -StartNumber $StartNumber
  $rnd   = New-Random -Seed $Seed

  Ensure-RegistryHeader -Path $RegistryPath

  for ($i=1; $i -le $FileCount; $i++) {
    $ts = Get-Timestamp
    $fileName = "{0}_{1}_{2:D3}.csv" -f $FilePrefix, $ts, $i
    $outPath  = Join-Path $OutputDir $fileName

    $rows = New-Object System.Collections.Generic.List[object]
    for ($r=1; $r -le $RecordsPerFile; $r++) {
      $nm = Get-UniqueName -State $state -Rnd $rnd
      $num = Get-NextNumber -State $state
      $cdt = Get-RandomCreateDate -Rnd $rnd -MaxAgeDays $MaxAgeDays

      $rows.Add([pscustomobject]@{
        'Number'      = $num
        'First Name'  = $nm.'First Name'
        'Last Name'   = $nm.'Last Name'
        'Create Date' = $cdt
      })
    }

    # Write CSV (headers will be: Number,First Name,Last Name,Create Date)
    $rows | Export-Csv -LiteralPath $outPath -Encoding UTF8 -NoTypeInformation

    # Append registry lines with SHA-256 per row
    foreach ($row in $rows) {
      $key = "{0}|{1}|{2}|{3}" -f $row.'Number', $row.'First Name', $row.'Last Name', $row.'Create Date'
      $hash = Get-Sha256Hex -Text $key
      $csvLine = ('"{0}","{1}","{2}","{3}","{4}","{5}"' -f $hash, $row.'Number', $row.'First Name', $row.'Last Name', $row.'Create Date', $fileName)
      Add-Content -LiteralPath $RegistryPath -Value $csvLine -Encoding UTF8
    }

    Write-Host "Generated $RecordsPerFile rows -> $outPath" -ForegroundColor Green
  }

  Save-State -State $state -Path $StatePath
  Write-Host "State saved to: $StatePath" -ForegroundColor DarkCyan
  Write-Host "Registry appended: $RegistryPath" -ForegroundColor DarkCyan
}
catch {
  Write-Error $_.Exception.Message
  exit 1
}
