# Dedicated Pool Discovery and Assessment

> **Purpose**: Extract schema from Synapse Dedicated SQL Pool using DACPAC or catalog queries and classify objects by complexity (T1-T4)
> **Tools**: SqlPackage CLI, Python 3.10+, Azure CLI
> **Output**: `<pool>.dacpac`, SQL scripts, inventory, complexity assessment, and SQL Pool to Lakehouse gap reports

This phase is notebook-free. Do not use a discovery notebook or require notebook artifacts from the source or target workspace.

---

## Table of Contents

| Section | Description |
|---------|-------------|
| [§ DACPAC Extraction](#dacpac-extraction) | SqlPackage commands for schema export |
| [§ Script Extraction](#script-extraction) | Extract individual SQL files from DACPAC |
| [§ Schema Inventory](#schema-inventory) | Parse DACPAC to list tables, views, procedures |
| [§ Complexity Classification](#complexity-classification) | T1-T4 classification contract |
| [§ Assessment Report](#assessment-report) | Generate JSON report with conversion estimates |
| [§ Migration Gap Report](#migration-gap-report) | Required SQL Pool to Lakehouse compatibility findings |

---

## DACPAC Extraction

### Prerequisites

**Install SqlPackage**:
- Windows: [Download SqlPackage](https://learn.microsoft.com/sql/tools/sqlpackage/sqlpackage-download)
- Linux/Mac: `dotnet tool install -g microsoft.sqlpackage`

**Authentication**:
```powershell
# Azure AD auth (recommended)
az login
$token = az account get-access-token --resource https://database.windows.net --query accessToken -o tsv
```

### Extract Command

```powershell
# Define connection parameters
$synapseServer = "your-workspace.sql.azuresynapse.net"
$dedicatedPool = "your-pool-name"
$outputPath = "./dacpac_output"
New-Item -ItemType Directory -Force -Path $outputPath

# Extract DACPAC (Azure AD auth)
sqlpackage /Action:Extract `
    /SourceConnectionString:"Server=tcp:$synapseServer,1433;Database=$dedicatedPool;Encrypt=True" `
    /AccessToken:$token `
  /TargetFile:"$outputPath/$dedicatedPool.dacpac" `
  /p:ExtractAllTableData=False `
  /p:VerifyExtraction=True `
  /p:ExtractReferencedServerScopedElements=False `
  /DiagnosticsFile:"$outputPath/extract.log"

# Check exit code
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ DACPAC extracted successfully" -ForegroundColor Green
    $dacpacSize = (Get-Item "$outputPath/$dedicatedPool.dacpac").Length / 1MB
    Write-Host "   Size: $([math]::Round($dacpacSize, 2)) MB"
} else {
    Write-Host "❌ Extraction failed. Check extract.log" -ForegroundColor Red
    Get-Content "$outputPath/extract.log" | Select-Object -Last 20
    exit 1
}
```

Do not place SQL passwords in command arguments or generated migration artifacts. If Azure AD authentication is unavailable, stop and have the operator configure an approved secret-backed authentication flow outside this skill.

### Troubleshooting DACPAC Extraction

| Error | Cause | Resolution |
|-------|-------|------------|
| **Connection timeout** | Firewall blocks IP | Add client IP to Synapse firewall rules |
| **Authentication failed** | AAD token expired | Re-run `az login` |
| **SqlPackage not found** | Not in PATH | Use full path: `C:\Program Files\Microsoft SQL Server\160\DAC\bin\SqlPackage.exe` |
| **Cannot connect to server** | Pool is paused | Resume Dedicated Pool in Azure Portal |
| **Insufficient permissions** | Lacks metadata visibility | Grant database `CONNECT` and `VIEW DEFINITION`; add narrower catalog permissions only when a failed query proves they are needed |

---

## Script Extraction

### Read the DACPAC Model

`sqlpackage /Action:Script` produces a deployment script only when given a target and does not create one file per source object. Do not use it as the inventory source. Load the schema-only DACPAC with DacFx and emit one normalized source file plus one inventory record per model object.

Use `TSqlModel.GetObjects(DacQueryScopes.All)` and classify objects by their DacFx `ObjectType`. At minimum, include:

- schemas, tables, columns, data types, defaults, identities, sequences, and partition specifications
- primary, foreign, unique, and check constraints; indexes, columnstore indexes, statistics, and materialized views
- views, procedures, scalar/table functions, triggers, synonyms, and external objects
- users, roles, permissions, row-level security, masking, workload groups, and classifiers
- all model relationships and referenced-object identifiers, not regex-only `FROM` and `JOIN` matches

For each object, use DacFx properties and relationships as the authoritative metadata. Use `GetScript()` only to retain normalized source text for complexity and conversion analysis.

```csharp
// Illustrative core of the DacFx extractor
using Microsoft.SqlServer.Dac.Model;

var dacpac = TSqlModel.LoadFromDacpac("pool.dacpac");
var objects = dacpac.GetObjects(DacQueryScopes.All)
        .Where(o => !o.ObjectType.Name.StartsWith("BuiltIn", StringComparison.Ordinal));

foreach (var sourceObject in objects)
{
        var objectType = sourceObject.ObjectType.Name;
        var objectName = sourceObject.Name?.ToString() ?? "";
        var sourceText = sourceObject.GetScript() ?? "";
        // Serialize DacFx properties, relationships, and source text into the
        // canonical inventory record. Do not infer names or dependencies by regex.
}
```

### Supplement with Read-Only Catalog Queries

A DACPAC does not represent every operational or external dependency. Query source catalogs read-only and merge results into the same inventory. Record each query, timestamp, success/failure, and error text so missing permissions become visible assessment blind spots.

Required catalog coverage includes `sys.schemas`, `sys.objects`, `sys.columns`, `sys.types`, `sys.default_constraints`, `sys.check_constraints`, `sys.key_constraints`, `sys.foreign_keys`, `sys.indexes`, `sys.index_columns`, `sys.partitions`, `sys.sql_modules`, `sys.sql_expression_dependencies`, `sys.database_principals`, `sys.database_permissions`, `sys.database_role_members`, `sys.security_policies`, `sys.masked_columns`, `sys.external_tables`, `sys.external_data_sources`, `sys.external_file_formats`, `sys.database_scoped_credentials`, `sys.pdw_table_distribution_properties`, and available workload-management/catalog views.

Never query table rows. Catalog and definition metadata only are permitted.

---

## Schema Inventory

### Canonical Inventory Contract

Write `schema-inventory.json` and `schema-inventory.md` from the merged DacFx and catalog results. The JSON root is an object with an `objects` array and a `blindSpots` array. Every object record must include the stable source identifier, schema, name, type, normalized `sourceText` and source path when applicable, DacFx properties, dependency identifiers, catalog evidence, and any discovery warnings. Preserve failed metadata queries in `blindSpots`.

Before classification, verify that every required gap category in [dedicated-pool-gap-assessment.md](dedicated-pool-gap-assessment.md) has either collected evidence or an explicit query failure. An empty pool is valid and must produce an empty inventory, zero counts, and no conversion work; it must not be padded with sample objects.

**Output**:
- `schema-inventory.json` — structured data for downstream processing
- `schema-inventory.md` — human-readable summary

---

## Complexity Classification

### T1-T4 Classification Contract

Classify every source-bearing object from `schema-inventory.json`. Apply tiers from highest to lowest precedence; the first matching tier wins. Preserve every matched indicator as evidence rather than relying on an unexplained numeric score.

| Tier | Indicators | Conversion approach |
|---|---|---|
| T4 | Stored procedure; cursor or loop; temporary table; dynamic SQL; transaction; TRY/CATCH | Spark SQL notebook component planning; defer notebook cardinality and grouping until the post-discovery capacity and mapping assessment is approved |
| T3 | Window function; CTE; nested subquery; set operation; PIVOT/UNPIVOT | Individual conversion and focused semantic review |
| T2 | Join; CASE; GROUP BY; aggregate | Guided conversion with targeted tests |
| T1 | Simple projection/filter without a higher-tier indicator | Routine conversion and syntax validation |

Object size and a wide projection are supplemental effort indicators only; they must not lower a tier selected by a semantic indicator. Objects without source text remain in the inventory with classification `Unknown` and a reason that identifies the missing evidence.

Write both `complexity-report.json` and `complexity-report.md`. Each classified object must include its stable source identifier, schema, name, type, tier, matched indicators, line count, dependencies, and review status. The Markdown report must summarize tier counts and percentages, list all T4 and `Unknown` objects, and identify the conversion approach for each tier.

---

## Assessment Report

### Consolidated Discovery Report

Generate `assessment-report.json` from the canonical inventory and complexity report. Include:

- generation timestamp and source pool identifier
- total discovered objects and counts by type, schema, tier, and review status
- all discovery blind spots and failed metadata queries
- T4 and `Unknown` objects requiring manual review
- dependency blockers and recommended migration waves
- discovered procedure count and the inventory inputs required to project target notebook and workspace item totals
- effort assumptions and estimates, clearly labeled as planning estimates rather than commitments

If using the default planning heuristic, calculate estimated hours as $0.5T1 + 1T2 + 3T3 + 8T4$ and estimated days as hours divided by eight. Keep the coefficients in report metadata so reviewers can replace them. Never infer low effort for `Unknown` objects; report them separately until evidence is available.

---

## Migration Gap Report

After inventory and complexity classification, execute the complete [Dedicated Pool to Lakehouse Gap Assessment](dedicated-pool-gap-assessment.md). Produce:

- `migration-gap-report.json`
- `migration-gap-report.md`

The report must assess every discovered object across every required gap category, identify metadata-query failures and unknowns, and list concrete evidence and disposition for each finding. It must also compare `1:1`, `N:1`, and `N:N` procedure-notebook strategies using the discovered procedure inventory and projected workspace item demand. Present the Markdown report to the customer, then require the customer to provide and approve the complete mapping strategy and workspace placement before conversion. Complexity and effort reports do not satisfy this requirement.

---

## Summary

This discovery phase provides:
1. **DACPAC Extraction** — Schema export from Synapse Dedicated Pool
2. **Script Extraction** — Individual SQL files for each object
3. **Schema Inventory** — Structured metadata (type, dependencies, line count)
4. **Complexity Classification** — T1-T4 tiers with conversion approach
5. **Assessment Report** — Effort estimate and recommendations
6. **Migration Gap Report** — Complete SQL Pool to Lakehouse compatibility findings and customer approval

**Next Phase**: Proceed to [dedicated-pool-conversion.md](dedicated-pool-conversion.md) only after the migration gap report is complete and approved.
