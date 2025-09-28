<#
Import-Csv.ps1 (RAW history + validation staging)

Key points
- RAW history table is `${BaseName}_hist` (default base: raw_sourcefile -> raw_sourcefile_hist)
- Staging holds the VALIDATED rows for this run: `${BaseName}_staging`
- Work holds the raw rows from this file before validation: `${BaseName}_work`
- Errors holds rejected rows with reasons: `${BaseName}_errors`
- With -UseStaging: validate vs EXISTING history, then append ALL rows to history
- With direct import (no -UseStaging): load to history (optionally TRUNCATE it first)

Usage examples

# Staging validation (recommended) — SQL auth, trust self-signed cert
.\Import-Csv.ps1 `
  -SqlInstance "192.168.10.181" `
  -Database "ImportData" `
  -Schema "dbo" `
  -BaseName "raw_sourcefile" `
  -CsvPath "C:\path\test_records.csv" `
  -UseStaging `
  -AutoCreateTable `
  -UseSqlAuth -SqlUser "etl_user" `
  -TrustServerCertificate

# Direct to history (append) — Windows auth
.\Import-Csv.ps1 `
  -SqlInstance "sqlhost.yourdomain.local" `
  -Database "ImportData" `
  -Schema "dbo" `
  -BaseName "raw_sourcefile" `
  -CsvPath "C:\path\test_records.csv" `
  -AutoCreateTable `
  -TrustServerCertificate

# Direct to history, replace (truncate first)
.\Import-Csv.ps1 `
  -SqlInstance "192.168.10.181" `
  -Database "ImportData" `
  -Schema "dbo" `
  -BaseName "raw_sourcefile" `
  -CsvPath "C:\path\test_records.csv" `
  -Truncate `
  -AutoCreateTable `
  -UseSqlAuth -SqlUser "etl_user" `
  -TrustServerCertificate
#>

param(
    [string]$SqlInstance = "192.168.10.181",
    [string]$Database    = "ImportData",
    [string]$Schema      = "dbo",

    # Logical base name; the physical tables are derived:
    #   History:  ${BaseName}_hist
    #   Staging:  ${BaseName}_staging
    #   Work:     ${BaseName}_work
    #   Errors:   ${BaseName}_errors
    [string]$BaseName    = "raw_sourcefile",

    # CSV location (defaults next to this script)
    [string]$CsvPath     = "$(Split-Path -Parent $PSCommandPath)\test_records.csv",

    # QoL
    [string]$ProcessedDir = "$(Split-Path -Parent $CsvPath)\processed",
    [string]$ErrorDir     = "$(Split-Path -Parent $CsvPath)\error",
    [int]$RetainDays      = 30,

    # Behaviors
    [switch]$AutoCreateTable,   # create/ensure tables exist
    [switch]$UseStaging,        # validate into staging
    [switch]$Truncate,          # only applies to direct-to-history path
    [switch]$TrustServerCertificate,

    # Auth mode
    [switch]$UseSqlAuth,        # SQL Authentication
    [string]$SqlUser,
    [string]$SqlPassword
)

# -------- Derived names --------
$RawHistTable  = "${BaseName}_hist"
$WorkTable     = "${BaseName}_work"
$StagingTable  = "${BaseName}_staging"
$ErrorsTable   = "${BaseName}_errors"

# -------- Helpers --------
function Ensure-Directory {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}
function Get-Timestamp { Get-Date -Format 'yyyyMMdd_HHmmss' }

# -------- Setup --------
$ErrorActionPreference = 'Stop'
$loadStart = Get-Date
$rowsValid = 0
$status = "ERROR"
$msg = ""

Write-Host "Checking for dbatools module..." -ForegroundColor Cyan
if (-not (Get-Module -ListAvailable -Name dbatools)) {
    Write-Host "Installing dbatools (CurrentUser scope)..." -ForegroundColor Yellow
    Install-Module dbatools -Scope CurrentUser -Force -AllowClobber
}
Import-Module dbatools

# Ensure folders
Ensure-Directory -Path $ProcessedDir
Ensure-Directory -Path $ErrorDir

# Build SQL credential if requested
$SqlCredential = $null
if ($UseSqlAuth) {
    if (-not $SqlUser) { $SqlUser = Read-Host "SQL Username" }
    if ($SqlPassword) {
        $sec = ConvertTo-SecureString $SqlPassword -AsPlainText -Force
    } else {
        $sec = Read-Host "SQL Password" -AsSecureString
    }
    $SqlCredential = New-Object System.Management.Automation.PSCredential ($SqlUser, $sec)
}

# Connect once; reuse
try {
    $connectParams = @{
        SqlInstance   = $SqlInstance
        ErrorAction   = 'Stop'
        WarningAction = 'SilentlyContinue'
    }
    if ($SqlCredential)          { $connectParams['SqlCredential'] = $SqlCredential }
    if ($TrustServerCertificate) {
        $connectParams['EncryptConnection']      = $true
        $connectParams['TrustServerCertificate'] = $true
    }
    $server = Connect-DbaInstance @connectParams
    Write-Host "Connection to $SqlInstance ok." -ForegroundColor Green
}
catch {
    Write-Error "Cannot connect to SQL instance '$SqlInstance'. Error: $($_.Exception.Message)"
    Write-Host "Tip: -UseSqlAuth can bypass SSPI issues; or connect via DNS host matching SPN/certificate." -ForegroundColor Yellow
    exit 1
}

# --- Create log table if not exists ---
$createLogSql = @"
IF OBJECT_ID('[dbo].[etl_file_load_log]','U') IS NULL
BEGIN
  CREATE TABLE [dbo].[etl_file_load_log](
    [LogId]        BIGINT IDENTITY(1,1) PRIMARY KEY,
    [LoadStart]    DATETIME2(0) NOT NULL,
    [LoadEnd]      DATETIME2(0) NOT NULL,
    [ServerName]   SYSNAME      NULL,
    [DatabaseName] SYSNAME      NOT NULL,
    [SchemaName]   SYSNAME      NOT NULL,
    [TableName]    SYSNAME      NOT NULL,   -- physical target (history)
    [StagingUsed]  BIT          NOT NULL,
    [SourceFile]   NVARCHAR(400) NOT NULL,
    [RowsCopied]   INT          NULL,
    [Status]       NVARCHAR(20) NOT NULL,
    [Message]      NVARCHAR(4000) NULL
  );
END
"@
Invoke-DbaQuery -SqlInstance $server -Database $Database -Query $createLogSql

# --- Auto-create tables if requested ---
if ($AutoCreateTable) {
    $createHistorySql = @"
IF OBJECT_ID('[$Schema].[$RawHistTable]','U') IS NULL
BEGIN
  CREATE TABLE [$Schema].[$RawHistTable](
    [Number]      VARCHAR(20)  NOT NULL,
    [First Name]  VARCHAR(50)  NULL,
    [Last Name]   VARCHAR(50)  NULL,
    [Create Date] VARCHAR(50)  NULL
  );
END
"@
    Invoke-DbaQuery -SqlInstance $server -Database $Database -Query $createHistorySql

    $createWorkSql = @"
IF OBJECT_ID('[$Schema].[$WorkTable]','U') IS NULL
BEGIN
  CREATE TABLE [$Schema].[$WorkTable](
    [Number]      VARCHAR(20)  NOT NULL,
    [First Name]  VARCHAR(50)  NULL,
    [Last Name]   VARCHAR(50)  NULL,
    [Create Date] VARCHAR(50)  NULL
  );
END
"@
    Invoke-DbaQuery -SqlInstance $server -Database $Database -Query $createWorkSql

    $createStagingSql = @"
IF OBJECT_ID('[$Schema].[$StagingTable]','U') IS NULL
BEGIN
  CREATE TABLE [$Schema].[$StagingTable](
    [Number]      VARCHAR(20)  NOT NULL,
    [First Name]  VARCHAR(50)  NULL,
    [Last Name]   VARCHAR(50)  NULL,
    [Create Date] VARCHAR(50)  NULL
  );
END
"@
    Invoke-DbaQuery -SqlInstance $server -Database $Database -Query $createStagingSql

    $createErrorsSql = @"
IF OBJECT_ID('[$Schema].[$ErrorsTable]','U') IS NULL
BEGIN
  CREATE TABLE [$Schema].[$ErrorsTable](
    [Number]      VARCHAR(20)   NULL,
    [First Name]  VARCHAR(50)   NULL,
    [Last Name]   VARCHAR(50)   NULL,
    [Create Date] VARCHAR(50)   NULL,
    [Reason]      NVARCHAR(200) NOT NULL,
    [LoadTs]      DATETIME2(0)  NOT NULL CONSTRAINT DF_${ErrorsTable}_LoadTs DEFAULT SYSDATETIME()
  );
END
"@
    Invoke-DbaQuery -SqlInstance $server -Database $Database -Query $createErrorsSql
}

# --- Paths that differ by mode ---
if ($UseStaging) {
    # Truncate per-run tables
    Invoke-DbaQuery -SqlInstance $server -Database $Database -Query "IF OBJECT_ID('[$Schema].[$WorkTable]','U') IS NOT NULL TRUNCATE TABLE [$Schema].[$WorkTable];"
    Invoke-DbaQuery -SqlInstance $server -Database $Database -Query "IF OBJECT_ID('[$Schema].[$StagingTable]','U') IS NOT NULL TRUNCATE TABLE [$Schema].[$StagingTable];"

    # Import CSV into WORK
    Write-Host "Importing CSV: $CsvPath -> [$Schema].[$WorkTable] (work)" -ForegroundColor Cyan
    $result = Import-DbaCsv -Path $CsvPath -SqlInstance $server -Database $Database -Schema $Schema -Table $WorkTable -Delimiter ',' -Encoding UTF8 -BatchSize 50000

    # Validate work against EXISTING history (before appending)
    $validateSql = @"
SET NOCOUNT ON;

-- Intra-file duplicates (within WORK)
WITH dup_in_file AS (
  SELECT [Number]
  FROM [$Schema].[$WorkTable]
  GROUP BY [Number]
  HAVING COUNT(*) > 1
),
-- Historical duplicates vs HISTORY table
dup_hist AS (
  SELECT w.[Number]
  FROM [$Schema].[$WorkTable] w
  INNER JOIN [$Schema].[$RawHistTable] h
    ON h.[Number] = w.[Number]
  GROUP BY w.[Number]
),
-- Invalid dates
bad_date AS (
  SELECT [Number]
  FROM [$Schema].[$WorkTable]
  WHERE TRY_CONVERT(datetime2(0), [Create Date]) IS NULL
)

-- Errors (can have multiple reasons per row)
INSERT INTO [$Schema].[$ErrorsTable]([Number],[First Name],[Last Name],[Create Date],[Reason])
SELECT w.[Number], w.[First Name], w.[Last Name], w.[Create Date], 'Duplicate in file'
FROM [$Schema].[$WorkTable] w
JOIN dup_in_file d ON d.[Number] = w.[Number];

INSERT INTO [$Schema].[$ErrorsTable]([Number],[First Name],[Last Name],[Create Date],[Reason])
SELECT w.[Number], w.[First Name], w.[Last Name], w.[Create Date], 'Duplicate vs history'
FROM [$Schema].[$WorkTable] w
WHERE w.[Number] IN (SELECT [Number] FROM dup_hist);

INSERT INTO [$Schema].[$ErrorsTable]([Number],[First Name],[Last Name],[Create Date],[Reason])
SELECT w.[Number], w.[First Name], w.[Last Name], w.[Create Date], 'Invalid Create Date'
FROM [$Schema].[$WorkTable] w
WHERE w.[Number] IN (SELECT [Number] FROM bad_date);

-- STAGING: keep only good rows
TRUNCATE TABLE [$Schema].[$StagingTable];

INSERT INTO [$Schema].[$StagingTable]([Number],[First Name],[Last Name],[Create Date])
SELECT w.[Number], w.[First Name], w.[Last Name], w.[Create Date]
FROM [$Schema].[$WorkTable] w
LEFT JOIN dup_in_file d ON d.[Number] = w.[Number]
LEFT JOIN dup_hist    h ON h.[Number] = w.[Number]
WHERE d.[Number] IS NULL
  AND h.[Number] IS NULL
  AND TRY_CONVERT(datetime2(0), w.[Create Date]) IS NOT NULL;

-- Return counts
SELECT
  (SELECT COUNT(*) FROM [$Schema].[$WorkTable])        AS RowsInFile,
  (SELECT COUNT(*) FROM [$Schema].[$StagingTable])     AS ValidRows,
  (SELECT COUNT(*) FROM [$Schema].[$ErrorsTable] WHERE Reason='Duplicate in file')   AS DupsInFile,
  (SELECT COUNT(*) FROM [$Schema].[$ErrorsTable] WHERE Reason='Duplicate vs history')AS DupsVsHist,
  (SELECT COUNT(*) FROM [$Schema].[$ErrorsTable] WHERE Reason='Invalid Create Date') AS BadDate;
"@
    $rs = Invoke-DbaQuery -SqlInstance $server -Database $Database -Query $validateSql
    $rowsValid = try { [int]($rs.ValidRows | Select-Object -First 1) } catch { 0 }

    # Append ALL rows (good + bad) to RAW HISTORY for audit trail
    Invoke-DbaQuery -SqlInstance $server -Database $Database -Query @"
INSERT INTO [$Schema].[$RawHistTable]([Number],[First Name],[Last Name],[Create Date])
SELECT [Number],[First Name],[Last Name],[Create Date]
FROM [$Schema].[$WorkTable];
"@

    $status = "SUCCESS"
    $msg = "File -> WORK=$($rs.RowsInFile[0]); VALID -> STAGING=$($rs.ValidRows[0]); Errors: DupsInFile=$($rs.DupsInFile[0]), DupsVsHist=$($rs.DupsVsHist[0]), BadDate=$($rs.BadDate[0]). History appended."
}
else {
    # Direct to history
    if ($Truncate) {
        Write-Host "Truncating [$Schema].[$RawHistTable]..." -ForegroundColor Yellow
        Invoke-DbaQuery -SqlInstance $server -Database $Database -Query "IF OBJECT_ID('[$Schema].[$RawHistTable]','U') IS NOT NULL TRUNCATE TABLE [$Schema].[$RawHistTable];"
    }

    Write-Host "Importing CSV: $CsvPath -> [$Schema].[$RawHistTable] (HISTORY)" -ForegroundColor Cyan
    $result = Import-DbaCsv -Path $CsvPath -SqlInstance $server -Database $Database -Schema $Schema -Table $RawHistTable -Delimiter ',' -Encoding UTF8 -BatchSize 50000
    $rowsValid = if ($result -and ($result.PSObject.Properties.Name -contains 'RowsCopied')) { [int]$result.RowsCopied } else { 0 }
    $status = "SUCCESS"
    $msg = "Loaded $rowsValid rows -> HISTORY (direct)."
}

# --- Move CSV to processed or error with timestamp ---
$timestamp = Get-Timestamp
$srcName   = Split-Path -Leaf $CsvPath
$base      = [System.IO.Path]::GetFileNameWithoutExtension($srcName)
$ext       = [System.IO.Path]::GetExtension($srcName)

try {
    if ($status -eq "SUCCESS") {
        $dest = Join-Path $ProcessedDir "$base.$timestamp$ext"
        Move-Item -LiteralPath $CsvPath -Destination $dest -Force
        # Retention cleanup
        Get-ChildItem -LiteralPath $ProcessedDir -File | Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$RetainDays) } | Remove-Item -Force -ErrorAction SilentlyContinue
    } else {
        $dest = Join-Path $ErrorDir "$base.$timestamp$ext"
        Move-Item -LiteralPath $CsvPath -Destination $dest -Force
        Get-ChildItem -LiteralPath $ErrorDir -File | Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$RetainDays) } | Remove-Item -Force -ErrorAction SilentlyContinue
    }
} catch {
    $msg += " | File move warning: $($_.Exception.Message)"
}

$loadEnd = Get-Date

# --- Write log row ---
$insLog = @"
INSERT INTO [dbo].[etl_file_load_log]
  ([LoadStart],[LoadEnd],[ServerName],[DatabaseName],[SchemaName],[TableName],
   [StagingUsed],[SourceFile],[RowsCopied],[Status],[Message])
VALUES
  (@LoadStart,@LoadEnd,@ServerName,@DatabaseName,@SchemaName,@TableName,
   @StagingUsed,@SourceFile,@RowsCopied,@Status,@Message);
"@
Invoke-DbaQuery -SqlInstance $server -Database $Database -Query $insLog -SqlParameter @{
    LoadStart   = $loadStart
    LoadEnd     = $loadEnd
    ServerName  = $server.ComputerName
    DatabaseName= $Database
    SchemaName  = $Schema
    TableName   = $RawHistTable
    StagingUsed = [int][bool]$UseStaging
    SourceFile  = $srcName
    RowsCopied  = $rowsValid
    Status      = $status
    Message     = $msg
}

# Final output
if ($status -eq "SUCCESS") {
    Write-Host $msg -ForegroundColor Green
    exit 0
} else {
    Write-Error $msg
    exit 1
}
