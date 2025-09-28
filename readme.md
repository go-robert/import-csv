````markdown
# Import-Csv.ps1 — CSV → SQL History + Validation Staging (dbatools)

A production-ready PowerShell script for loading CSV files into SQL Server using the **dbatools** module, with an opinionated data-ingestion pattern:

- **History (immutable)**: appends every row from every file to `${BaseName}_hist`
- **Staging (validated)**: keeps this run’s *clean* rows in `${BaseName}_staging`
- **Errors (audit)**: records rejected rows & reasons in `${BaseName}_errors`
- **Work (scratch)**: per-run landing table `${BaseName}_work`, truncated every run
- **File handling**: moves CSV to `processed/` or `error/` (timestamped)
- **Logging**: per-run entry in `dbo.etl_file_load_log` (duration, status, counts, message)
- **Auth**: Windows/Integrated (default) or SQL authentication (`-UseSqlAuth`)
- **TLS**: `-TrustServerCertificate` for dev/test w/ self-signed certs

---

## Contents

- [Why this pattern?](#why-this-pattern)
- [Architecture](#architecture)
  - [Flow (Mermaid diagram)](#flow-mermaid-diagram)
  - [Tables (Mermaid ER-style)](#tables-mermaid-er-style)
- [Prerequisites](#prerequisites)
- [Parameters](#parameters)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
- [Validation Rules](#validation-rules)
- [What gets created in SQL](#what-gets-created-in-sql)
- [What happens to the CSV file](#what-happens-to-the-csv-file)
- [Logging](#logging)
- [Verification Queries](#verification-queries)
- [Troubleshooting](#troubleshooting)
- [Extending the pipeline](#extending-the-pipeline)
- [FAQ](#faq)

---

## Why this pattern?

**Separate concerns**:

- **History** = immutable record of intake (auditable, reproducible)
- **Staging** = clean snapshot for downstream transforms
- **Errors** = diagnostics and feedback loop to data producers

This avoids risky direct loads into curated tables and gives you strong **observability**, **idempotency controls**, and **quality gates**.

---

## Architecture

### Flow (Mermaid diagram)

```mermaid
flowchart LR
    subgraph FS[File System]
      A[CSV file]
    end

    subgraph SQL[SQL Server]
      W[[${BaseName}_work]]:::work
      H[[${BaseName}_hist]]:::hist
      S[[${BaseName}_staging]]:::stag
      E[[${BaseName}_errors]]:::err
      L[(dbo.etl_file_load_log)]:::log
    end

    A -->|Import-DbaCsv| W
    W -->|Validate: dup-in-file, dup-vs-history, valid date| S
    W -->|Rejects with reasons| E
    W -->|Append all rows| H
    A -->|Move| P{Result}
    P -->|Success| Proc[processed/<timestamped>.csv]
    P -->|Error| Err[error/<timestamped>.csv]
    P --> L

    classDef work fill:#eef,stroke:#446
    classDef hist fill:#efe,stroke:#363
    classDef stag fill:#ffe,stroke:#663
    classDef err fill:#fee,stroke:#633
    classDef log fill:#ddd,stroke:#555
````

> **Note:** With `-UseStaging` **off**, we skip Work/Staging/Errors and load directly to History (optionally `-Truncate` first).

### Tables (Mermaid ER-style)

```mermaid
erDiagram
  RAW_WORK           { varchar(20) Number
                       varchar(50) First_Name
                       varchar(50) Last_Name
                       varchar(50) Create_Date }
  RAW_HISTORY        { varchar(20) Number
                       varchar(50) First_Name
                       varchar(50) Last_Name
                       varchar(50) Create_Date }
  RAW_STAGING        { varchar(20) Number
                       varchar(50) First_Name
                       varchar(50) Last_Name
                       varchar(50) Create_Date }
  RAW_ERRORS         { varchar(20) Number
                       varchar(50) First_Name
                       varchar(50) Last_Name
                       varchar(50) Create_Date
                       nvarchar(200) Reason
                       datetime2 LoadTs }
  ETL_LOG            { bigint LogId
                       datetime2 LoadStart
                       datetime2 LoadEnd
                       sysname ServerName
                       sysname DatabaseName
                       sysname SchemaName
                       sysname TableName
                       bit StagingUsed
                       nvarchar SourceFile
                       int RowsCopied
                       nvarchar Status
                       nvarchar Message }

  RAW_WORK      ||--o{ RAW_ERRORS  : "rejects -> errors"
  RAW_WORK      ||--o{ RAW_STAGING : "valid rows -> staging"
  RAW_WORK      ||--o{ RAW_HISTORY : "append snapshot to history"
  ETL_LOG       }o--|| RAW_HISTORY : "logs runs on target table"
```

*Actual names derive from `-BaseName` (default `raw_sourcefile`):*

* History: `${BaseName}_hist` (e.g., `raw_sourcefile_hist`)
* Staging: `${BaseName}_staging`
* Work: `${BaseName}_work`
* Errors: `${BaseName}_errors`

---

## Prerequisites

* **PowerShell** 5.1+ (Windows) or PowerShell 7+
* **dbatools** PowerShell module (script installs if missing)
* **SQL Server** with a reachable instance
* Permissions to **create tables** and **insert** into the chosen database/schema
* CSV headers exactly:

  * `"Number"`, `"First Name"`, `"Last Name"`, `"Create Date"`
    (Double quotes not required in the file; the script maps on header names.)

---

## Parameters

| Parameter                 | Type   |              Default | Required | Description                                                               |
| ------------------------- | ------ | -------------------: | :------: | ------------------------------------------------------------------------- |
| `-SqlInstance`            | string |   `"192.168.10.181"` |     ✅    | SQL Server instance (hostname or IP; prefer DNS for Kerberos/TLS)         |
| `-Database`               | string |       `"ImportData"` |     ✅    | Database name                                                             |
| `-Schema`                 | string |              `"dbo"` |     ✅    | Schema name                                                               |
| `-BaseName`               | string |   `"raw_sourcefile"` |     ✅    | Logical base; tables are `${BaseName}_hist/_work/_staging/_errors`        |
| `-CsvPath`                | string | `.\test_records.csv` |     ✅    | Path to CSV file                                                          |
| `-AutoCreateTable`        | switch |                  off |          | Creates tables if they do not exist                                       |
| `-UseStaging`             | switch |                  off |          | Enables validation flow (Work → Staging + Errors, then append to History) |
| `-Truncate`               | switch |                  off |          | **Direct mode only**: truncate History before import                      |
| `-UseSqlAuth`             | switch |                  off |          | Use SQL authentication (else Windows/Integrated)                          |
| `-SqlUser`                | string |                    — |          | SQL login; prompts if omitted when `-UseSqlAuth`                          |
| `-SqlPassword`            | string |                    — |          | SQL password; prompts securely if omitted (plaintext discouraged)         |
| `-TrustServerCertificate` | switch |                  off |          | For dev/test TLS: encrypt channel but skip cert validation                |
| `-ProcessedDir`           | string | `processed` near CSV |          | Where successful CSVs are moved (timestamped)                             |
| `-ErrorDir`               | string |     `error` near CSV |          | Where failed CSVs are moved (timestamped)                                 |
| `-RetainDays`             | int    |                 `30` |          | Cleanup older files in processed/error                                    |

---

## Quick Start

**Recommended** (validation path with SQL auth & self-signed TLS):

```powershell
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
# (Will prompt for password)
```

**Direct to history** (append) with Windows auth:

```powershell
.\Import-Csv.ps1 `
  -SqlInstance "sqlhost.yourdomain.local" `
  -Database "ImportData" `
  -Schema "dbo" `
  -BaseName "raw_sourcefile" `
  -CsvPath "C:\path\test_records.csv" `
  -AutoCreateTable
```

**Direct to history** (replace snapshot):

```powershell
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
```

---

## Usage Examples

### 1) Validate each file, keep a clean staging snapshot

```powershell
.\Import-Csv.ps1 `
  -SqlInstance "sql01.contoso.local" `
  -Database "ImportData" `
  -Schema "dbo" `
  -BaseName "raw_sourcefile" `
  -CsvPath ".\drop\contacts_2025-09-28.csv" `
  -UseStaging -AutoCreateTable `
  -UseSqlAuth -SqlUser "etl_user"
```

### 2) Append-only history for audit

```powershell
.\Import-Csv.ps1 `
  -SqlInstance "sql01.contoso.local" `
  -Database "ImportData" `
  -Schema "dbo" `
  -BaseName "contact_intake" `
  -CsvPath ".\drop\contacts.csv" `
  -AutoCreateTable
# Loads into contact_intake_hist
```

### 3) Integrate with a generator

If you use a generator like `New-FakeContacts.ps1` to produce many CSVs, loop them:

```powershell
Get-ChildItem .\out\*.csv | ForEach-Object {
  .\Import-Csv.ps1 `
    -SqlInstance "sql01" -Database "ImportData" -Schema "dbo" -BaseName "raw_sourcefile" `
    -CsvPath $_.FullName -UseStaging -AutoCreateTable -UseSqlAuth -SqlUser "etl_user" -TrustServerCertificate
}
```

---

## Validation Rules

When `-UseStaging` is set, the script applies **three** default checks to the per-run **Work** table:

1. **Duplicate in file** (intra-file): duplicate `Number` within the current CSV
2. **Duplicate vs history**: `Number` already exists in `${BaseName}_hist`
3. **Invalid Create Date**: `TRY_CONVERT(datetime2, [Create Date])` is `NULL`

* **Valid rows** → `${BaseName}_staging`
* **Rejected rows** → `${BaseName}_errors` with a **Reason**
* **All rows (good + bad)** → appended to `${BaseName}_hist` (audit trail)

> You can add more rules (e.g., required fields, regex checks) by inserting SQL after the import to Work.

---

## What gets created in SQL

If `-AutoCreateTable` is used:

* `${BaseName}_hist` — **History** (immutable append)
* `${BaseName}_work` — **Work** (truncated each run)
* `${BaseName}_staging` — **Staging** (validated snapshot)
* `${BaseName}_errors` — **Errors** (rejected rows & reasons)
* `dbo.etl_file_load_log` — **Run log** (one row per file)

**Column definitions** (as requested):

```sql
CREATE TABLE [$Schema].[${BaseName}_hist](
  [Number]      VARCHAR(20)  NOT NULL,
  [First Name]  VARCHAR(50)  NULL,
  [Last Name]   VARCHAR(50)  NULL,
  [Create Date] VARCHAR(50)  NULL
);
-- Similar shape for _work, _staging; _errors adds [Reason], [LoadTs]
```

> Tip: Later, add computed typed columns (e.g., `CreateDate_dt AS TRY_CONVERT(datetime2(0), [Create Date]) PERSISTED`) and index them for downstream performance.

---

## What happens to the CSV file

* On **success**, the source file moves to:
  `processed/<originalName>.<yyyyMMdd_HHmmss>.csv`
* On **error**, it moves to:
  `error/<originalName>.<yyyyMMdd_HHmmss>.csv`
* Files older than `-RetainDays` (default **30**) are deleted from each folder.

---

## Logging

Each run writes a row to `dbo.etl_file_load_log`:

* `LoadStart`, `LoadEnd`, `ServerName`, `DatabaseName`, `SchemaName`, `TableName`
* `StagingUsed` (bit), `SourceFile`, `RowsCopied` (valid rows in staging or rows to history)
* `Status` (`SUCCESS` / `ERROR`)
* `Message` (human-readable summary: counts, reasons)

---

## Verification Queries

**How many rows were valid this run?**

```sql
SELECT COUNT(*) AS ValidRows
FROM dbo.raw_sourcefile_staging;
```

**What errors occurred and why?**

```sql
SELECT Reason, COUNT(*) AS Cnt
FROM dbo.raw_sourcefile_errors
GROUP BY Reason
ORDER BY Cnt DESC;
```

**Did the file land in history?**

```sql
SELECT TOP (10) *
FROM dbo.raw_sourcefile_hist
ORDER BY [Number];
```

**Recent run log**

```sql
SELECT TOP (20) *
FROM dbo.etl_file_load_log
ORDER BY LogId DESC;
```

---

## Troubleshooting

**“The certificate chain was issued by an authority that is not trusted.”**

* Use `-TrustServerCertificate` (dev/test only), **or** install a properly trusted TLS certificate on SQL Server and connect via a **DNS name** matching the cert.

**“The target principal name is incorrect. Cannot generate SSPI context.”**

* Kerberos/SSPI issue (common when using **IP**). Solutions:

  * Use `-UseSqlAuth` to bypass SSPI, or
  * Connect with the **host name** (SPN must match), or
  * Fix SPNs for the SQL Service account.

**“Invalid value for key 'Encrypt'.”**

* Caused by handcrafted connection strings. This script **lets dbatools** set encryption options for you.

**Permission errors**

* Ensure the login can **create tables** (if `-AutoCreateTable`) and **insert** into the schema.

**Wrong column names**

* CSV header names must match: `Number`, `First Name`, `Last Name`, `Create Date`.

---

## Extending the pipeline

* **More validators**: e.g., non-blank names, allowed characters, future/old date cutoffs.
* **Dedup strategies**: Persist a “canonical” table with surrogate keys.
* **Typed columns**: Add computed typed columns in History/Staging for faster analytics.
* **Indexes**: Add indexes on `[Number]` and parsed date columns.
* **Metrics**: Extend `etl_file_load_log` with more counters (bad dates, dup counts, etc.).
* **Orchestration**: Wrap in a scheduler (SQL Agent / Task Scheduler / Azure DevOps) and notify on failures.

---

## FAQ

**Why append bad rows to history?**
Auditability. History is a true record of what arrived; Staging is the curated snapshot you trust for downstream work. Errors table explains why rows were excluded.

**Can I skip Staging?**
Yes. Omit `-UseStaging` to load directly into History (optionally `-Truncate` it to replace).

**Why do columns have spaces?**
To match your source contract exactly. Downstream models can map or compute typed/normalized columns.

**How do I seed test files?**
Pair this with a generator script (e.g., `New-FakeContacts.ps1`) to produce unique CSVs. Then batch-import them.

---

## Appendix: Typical Run Output

```
Checking for dbatools module...
Connection to 192.168.10.181 ok.
Importing CSV: C:\drop\contacts_2025-09-28.csv -> [dbo].[raw_sourcefile_work] (work)
File -> WORK=50; VALID -> STAGING=49; Errors: DupsInFile=1, DupsVsHist=0, BadDate=0. History appended.
Moved to processed/contacts_2025-09-28.20250928_0815.csv
Logged run: dbo.etl_file_load_log (Status=SUCCESS)
```

---

```
```
