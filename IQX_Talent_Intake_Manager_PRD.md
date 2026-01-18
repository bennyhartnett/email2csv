# PRD: IQX Veteran Talent Intake and Bulk Import Manager

**Working name:** Talent Intake Manager for IQX

**Document version:** 1.0

**Primary outcome:** Convert the current Excel-heavy, manual, recurring workflow for VIPER veteran trade-interest lists into a repeatable, auditable, low-error pipeline that produces IQX-ready bulk import jobs and supports error remediation.

**Primary workflow source:** TRNG IQX - H2H Lists Script for Video 2025-12-10

---

## 1. Executive summary

VIPER sends recurring spreadsheets such as Helmets to Hardhats career seeker trade-interest lists for multiple unions and districts. Each source file typically contains multiple weeks of redundant data. A Talent Operations user currently:

- downloads email attachments,
- saves originals separately,
- renames files using the date of the latest veteran signup,
- copies only incremental rows since the last upload,
- cleans and normalizes fields,
- deduplicates across sources,
- generates an IQX bulk import CSV,
- uploads to IQX,
- reviews job results and fixes failures such as duplicate emails and invalid ZIP codes.

The proposed product, **Talent Intake Manager**, will provide:

1. Ingestion from email and or manual upload with automatic archival and versioning.
2. Parsing plus incremental extraction across multiple sources.
3. Normalization and data quality enforcement for names, phones, ZIP codes, profession mapping, and comments.
4. Deduplication with configurable rules and a human-in-the-loop review interface.
5. IQX bulk import export that matches the IQX template.
6. Bulk import reconciliation support including error intake, fix guidance, and corrected export generation.

---

## 2. Background and current state

### 2.1 Current workflow highlights

The training workflow describes a repeatable operating procedure that includes:

- Monthly folder setup with a dedicated originals folder to preserve received attachments and avoid re-finding emails.
- Downloading VIPER attachments that may include H2H, VEEP, DOL, WIX, and other lists.
- Copying forward two processing templates from the prior month:
  - an Excel analysis workbook, and
  - a CSV bulk import template.
- Renaming received files and processing templates using the date of the last veteran signup in the reporting window, in `YYYY-MM-DD` format, to ensure clear sorting and traceability.
- Incremental extraction because each H2H source file contains approximately four weeks of data and therefore overlaps prior uploads.
- Normalization steps including:
  - forcing ZIP codes to remain text to preserve leading zeros,
  - standardizing phone formats,
  - converting trade and profession values to valid O*NET profession titles,
  - injecting service branch into internal comments because IQX does not have a dedicated service-branch field,
  - setting availability dates and location radius defaults.
- Deduplication and source precedence decisions with audit notes.
- IQX bulk import and post-import remediation.

### 2.2 Why a dedicated product is required

The current workflow is operationally effective but inefficient and fragile:

- It depends on manual spreadsheet manipulation.
- It requires nuanced, tacit knowledge for incremental extraction and dedupe.
- It produces import failures that must be resolved in IQX and in the working files.
- It has limited structured auditability beyond file naming conventions and comments.

---

## 3. Problem statement

### 3.1 User problems

- High time cost and cognitive load due to repetitive copy, paste, sort, and format steps.
- Recurrent redundancy because source lists overlap multiple weeks.
- Data quality failure modes that generate IQX rejects, including:
  - duplicate emails,
  - invalid or mis-entered ZIP codes,
  - inconsistent phone formats,
  - profession values not matching O*NET title requirements.
- Low traceability: difficult to reconstruct which rows were imported, excluded, deduped, or edited and why.

### 3.2 Business problems

- Delays in activating veteran talent profiles.
- Reduced matching quality caused by inconsistent field normalization.
- Increased operational risk from insufficient audit trails for PII handling and transformation history.

---

## 4. Goals and non-goals

### 4.1 Goals

1. Reduce manual processing by automating ingestion, incremental extraction, normalization, dedupe preparation, and CSV generation.
2. Reduce IQX import failures with pre-validation and guided remediation.
3. Improve consistency across sources and across months.
4. Provide strong auditability, including preserved originals, transformation logs, and dedupe decision logs.
5. Enable faster remediation of failures via a structured error queue and correction exports.

### 4.2 Non-goals for version 1

- Replacing IQX as the system of record.
- Introducing new schema fields inside IQX.
- Sending marketing or onboarding messages.
- Managing downstream recruiting lifecycle states beyond bulk import preparation and reconciliation.

---

## 5. Stakeholders and users

### 5.1 Personas

1. **Talent Operations Specialist**
   - Ingests lists, prepares bulk imports, uploads to IQX, fixes failures.

2. **Talent Operations Manager**
   - Needs throughput metrics, data quality monitoring, and audit trails.

3. **Data or Technical Administrator**
   - Maintains profession mappings and source configurations, monitors system health.

4. **Compliance and Quality Assurance**
   - Requires traceability for data provenance, edits, and exports.

---

## 6. User journeys

### 6.1 Journey A: Standard weekly or monthly import

1. User creates a new batch.
2. User uploads VIPER attachments, or the system pulls attachments from a configured inbox.
3. The system identifies source type per file and archives originals immutably.
4. The system computes incremental new records since the last imported watermark for each source.
5. The system normalizes records and runs validations.
6. The system identifies duplicates and presents a resolution workbench.
7. User resolves any ambiguous conflicts.
8. The system generates an IQX-ready CSV and a review report.
9. User uploads the CSV into IQX.
10. User records the bulk import job metadata in the batch record.

### 6.2 Journey B: Import failures and remediation

1. User receives IQX bulk import results and identifies failed records.
2. User imports the failures into the system or enters them manually.
3. The system categorizes failures and recommends fixes.
4. User applies fixes and generates a corrected CSV.
5. User resubmits corrections in IQX and marks the batch issues resolved.

---

## 7. Scope and phased delivery

### 7.1 Phase 1 MVP

- Manual file upload for XLSX and CSV
- Parsing and canonicalization
- Incremental extraction with per-source watermarks
- Normalization and validation
- Deduplication workbench
- IQX CSV export
- Batch history and immutable originals archive

### 7.2 Phase 2 Email ingestion and richer reconciliation

- Email integration to auto-detect VIPER messages and pull attachments
- Automated job packaging with review report generation
- Structured remediation flows and correction exports

### 7.3 Phase 3 Deeper IQX integration

- If IQX supports API capabilities, automate job creation, status retrieval, and failed-row retrieval

---

## 8. Functional requirements

### 8.1 Intake and archival

#### FR-1 Multi-source ingestion

- Support manual upload of one or more XLSX or CSV files.
- Support batch ingestion for multiple attachments at once.
- Phase 2: support email ingestion from a configured VIPER mailbox.

**Acceptance criteria**

- User can upload multiple files in one batch.
- System stores sender, received time, file name, file hash, and inferred source type.

#### FR-2 Immutable originals archive

- Store all original attachments without modification.
- Enforce versioning and prevent overwrite.

**Acceptance criteria**

- Originals can be re-downloaded later.
- Every derived record links back to a source file and source row provenance.

#### FR-3 Batch versioning

- System assigns a batch identifier and creation timestamp.
- Batch has a primary date that defaults to the latest veteran signup date observed in the inputs.

**Acceptance criteria**

- Re-running the same batch inputs produces identical outputs.

---

### 8.2 Parsing and canonicalization

#### FR-4 Robust file parsing

- Parse H2H-style list columns such as record identifier, names, create date, phone, email, branch of service, trade of interest, address fields, and postal code.
- Support minor header variations and column order differences.

**Acceptance criteria**

- Missing required columns are surfaced as blocking errors.
- Parser produces a preview with row counts and field mapping confidence.

#### FR-5 Canonical record schema

- Store parsed rows into a canonical schema with provenance per field.

**Acceptance criteria**

- Each field indicates its source file and source column.

---

### 8.3 Incremental extraction

#### FR-6 Watermark tracking per source

- For each source type, maintain a last-imported watermark.
- Watermark strategy:
  - primary: create date
  - secondary: record identifier

**Acceptance criteria**

- System can filter out redundant prior weeks automatically.
- User can override the cutoff date and record a reason.

#### FR-7 Increment preview

- Present included and excluded counts by source.
- Provide sample rows for excluded records.

**Acceptance criteria**

- User can approve or adjust before proceeding.

---

### 8.4 Normalization and validation

#### FR-8 Name normalization

- Apply proper noun capitalization.
- Support hyphenation for multi-part names when needed to prevent profile display and import issues.

**Acceptance criteria**

- System displays a change log for modified names and allows user override.

#### FR-9 Branch of service injection

- Generate internal comment prefix in the format `Service: <Branch>`.
- Controlled vocabulary is configurable.

**Acceptance criteria**

- All records with a source branch of service produce a standardized comment value.

#### FR-10 Profession normalization using O*NET titles

- Profession values must match valid O*NET profession titles.
- Apply required replacements, including:
  - `Electricians/Lineman` to `Electricians`
  - `Ironworkers` to `Structural Iron and Steel Workers`

**Acceptance criteria**

- Unmapped professions are blocking and require user mapping or admin update.

#### FR-11 Industry and talent price category defaults

- Apply default mappings by profession, including:
  - Electricians to Industry `22 Utilities` and Talent Price Category `A`
  - Structural Iron and Steel Workers to Industry `23 Construction` and Talent Price Category `A`

**Acceptance criteria**

- Mapping table is configurable and versioned.

#### FR-12 Phone normalization

- Normalize US numbers into a consistent readable format.
- Preserve overseas numbers and flag for review.

**Acceptance criteria**

- Invalid phone formats are surfaced as warnings or blocking issues based on configurable policy.

#### FR-13 Email validation and uniqueness

- Validate syntax.
- Detect duplicates within the batch.

**Acceptance criteria**

- System blocks export if duplicate emails remain unresolved.

#### FR-14 ZIP code handling

- Treat ZIP as text to preserve leading zeros.
- Validate format as five digits, optionally ZIP plus four.
- Provide correction suggestions when the city and state conflict with ZIP lookups.

**Acceptance criteria**

- Invalid ZIP codes are blocking unless user overrides with a reason.

#### FR-15 Availability dates and location radius

- Default values:
  - available date equals next working day
  - end date equals available date plus one year
  - location radius equals 100

**Acceptance criteria**

- User can override defaults at the batch level and the row level.

#### FR-16 Invitation status

- Support an invitation status field aligned to the current bulk import practice of setting an invited marker value.

**Acceptance criteria**

- Export includes the invited marker when required by the IQX import template.

---

### 8.5 Deduplication and merge

#### FR-17 Duplicate candidate detection

- Identify duplicates across sources using a tiered matching strategy:
  1. exact email match
  2. exact first name plus last name plus ZIP match
  3. fuzzy name plus corroborating phone or ZIP match with configurable thresholds

**Acceptance criteria**

- System produces a duplicate queue with confidence scores.

#### FR-18 Human-in-the-loop resolution

- Provide side-by-side comparison for candidate duplicates.
- Allow user to choose a primary record and merge selected fields.
- Support multi-source tagging so both sources can be displayed downstream.
- Allow notes for scenarios such as a veteran reapplying with a different email.

**Acceptance criteria**

- Every dedupe decision is logged with user, timestamp, and rationale.

#### FR-19 Source precedence rules

- Provide configurable precedence suggestions.
- Always allow overrides.

**Acceptance criteria**

- Precedence logic is transparent, logged, and auditable.

---

### 8.6 IQX export and packaging

#### FR-20 IQX CSV export

- Export a CSV that matches the IQX bulk import template fields in both header names and formats.
- Expected export concepts include:
  - invited marker
  - external source
  - internal comments
  - first name and last name
  - email and phone
  - availability and end dates
  - ZIP code and radius
  - profession, industry, and talent price category
  - optional clearance fields

**Acceptance criteria**

- Export passes all blocking validations.
- Export is deterministic per batch.

#### FR-21 Review report

- Produce a companion report that includes:
  - counts by source
  - excluded counts by watermark filtering
  - dedupe outcomes
  - all blocking issues and resolutions
  - transformation summary

**Acceptance criteria**

- User can download the report as CSV and PDF.

#### FR-22 Batch count reconciliation

- Compute and display counts that reconcile from ingestion through export.

**Acceptance criteria**

- Ingested minus excluded minus removed duplicates equals exported.

---

### 8.7 Reconciliation and remediation

#### FR-23 Import job tracking

- Record which export file was uploaded and when.
- Allow storage of an IQX bulk import job identifier.

**Acceptance criteria**

- User can locate any batch and its artifacts quickly.

#### FR-24 Failure intake

- Support entry of failed records and issue categories.
- Accept an uploaded structured failure report when available.

**Acceptance criteria**

- System creates an error queue with status.

#### FR-25 Guided fixes and correction exports

- Provide fix guidance for common issues:
  - duplicate email
  - invalid ZIP code
- Generate a corrected CSV and a remediation log.

**Acceptance criteria**

- System produces a correction artifact that can be resubmitted.

---

## 9. Non-functional requirements

### 9.1 Security and privacy

- Implement role-based access control.
- Encrypt data at rest and in transit.
- Log data access, exports, and dedupe decisions.
- Provide configurable retention policies for originals and derived artifacts.

### 9.2 Reliability and performance

- Handle hundreds to thousands of records per batch.
- Ensure idempotent re-runs and deterministic output.

### 9.3 Usability and operability

- Clear separation between blocking errors and warnings.
- Row-level provenance and change history.
- Minimal reliance on spreadsheets for the standard path.

---

## 10. Data model and field mapping

### 10.1 Canonical entities

#### SourceFile

- `id`
- `source_type` such as IBEW D4, IBEW D8, IBEW D9, Ironworkers
- `received_at`
- `original_filename`
- `archive_uri`
- `hash`

#### TalentRecord

- `source_record_id`
- `first_name`
- `last_name`
- `create_date`
- `email`
- `phone`
- `branch_of_service`
- `trade_of_interest`
- `zip_code`
- `street_address`
- `city`
- `state`
- `county`
- `source_tags` array
- `invited_marker`
- `internal_comments`
- `profession`
- `industry`
- `talent_price_category`
- `available_date`
- `until_date`
- `location_radius`
- `validation_status`
- `validation_issues` array
- `provenance` object

#### ImportBatch

- `id`
- `batch_date`
- `created_by`
- `created_at`
- `source_files` list
- `watermark_by_source`
- `counts` object
- `export_artifacts` object

#### DedupDecision

- `batch_id`
- `cluster_id`
- `chosen_record_id`
- `merged_fields`
- `rationale`
- `decided_by`
- `decided_at`

#### RemediationIssue

- `batch_id`
- `record_id`
- `issue_type`
- `suggested_fix`
- `status`
- `notes`

### 10.2 Field mapping examples

| Source field example | Canonical field | Export concept | Transform |
|---|---|---|---|
| First Name | first_name | First Name | Proper-case; hyphenation support |
| Last Name | last_name | Last Name | Proper-case; hyphenation support |
| Mobile Phone Number | phone | Phone | Normalize; flag invalid |
| Email | email | Email | Validate; enforce uniqueness |
| Branch of Service | branch_of_service | Internal Comments | Prefix as `Service: <Branch>` |
| Postal Code | zip_code | ZIP Code | Store as text; validate |
| Trade of Interest | trade_of_interest | Profession and related | Map to O*NET plus defaults |
| Create Date | create_date | Not exported | Watermark filtering |

---

## 11. User experience requirements

### 11.1 Primary screens

1. Batch creation and file upload
2. Parsing preview and source detection
3. Incremental extraction review
4. Data quality dashboard
5. Deduplication workbench
6. Export center
7. Remediation center

### 11.2 Auditability features

- Per-row change log with before and after values.
- Dedupe decision log with rationale.
- Export artifact retention with hashes.
- Original source retention.

---

## 12. Analytics and KPIs

### 12.1 Operational metrics

- Time from ingestion to export
- Records ingested, excluded, deduped, exported
- Pre-export validation failure rate
- Post-import failure rate as entered by users

### 12.2 Data quality metrics

- Percent of records with valid ZIP codes
- Percent with normalized phone numbers
- Percent mapped to valid O*NET professions
- Percent with service branch captured in internal comments

---

## 13. Dependencies and assumptions

### 13.1 Dependencies

- Stable VIPER email delivery and attachment formats
- Stable IQX bulk import template
- Ownership for profession and industry mapping maintenance

### 13.2 Assumptions

- Email uniqueness is enforced by IQX and causes import rejects.
- ZIP codes must meet IQX acceptance criteria and are rejected when invalid.
- The invited marker field is required or operationally useful in the IQX import template.

---

## 14. Risks and mitigations

1. IQX template changes
   - Mitigation: configurable template versions, schema checks, and export versioning.

2. Source file format drift
   - Mitigation: tolerant parsing, header mapping UI, and blocking errors for missing required fields.

3. False-positive dedupe merges
   - Mitigation: confidence scoring, human confirmation for low-confidence clusters, immutable decision logs.

4. PII handling risk
   - Mitigation: RBAC, encryption, retention policies, and detailed audit logs.

---

## 15. Release and rollout plan

### 15.1 Rollout approach

- Pilot one monthly cycle for the core sources.
- Compare time to export, dedupe outcomes, and IQX rejection rates against the baseline spreadsheet process.

### 15.2 Enablement

- Provide a short operating procedure mapping the legacy spreadsheet steps to the new screens.
- Provide audit and recovery guidance for where originals, exports, and decision logs are stored.

---

## 16. Open questions

1. Confirm the exact IQX bulk import CSV headers, required fields, and accepted formats.
2. Confirm accepted phone formats and normalization target.
3. Confirm ZIP code acceptance rules and whether non-US formats are ever permitted.
4. Define governance and workflow for updating O*NET profession mappings.
5. Determine whether an IQX API exists for import job creation and result retrieval.

---

## 17. Acceptance criteria for parity with the current workflow

Version 1 is considered parity-complete when a Talent Operations Specialist can:

- ingest the same monthly VIPER attachments,
- automatically isolate new rows since the prior upload,
- normalize service branch into internal comments,
- normalize professions to valid O*NET titles and apply defaults,
- preserve ZIP formatting and prevent leading-zero loss,
- detect and resolve duplicates with an auditable interface,
- export an IQX-ready CSV that yields materially fewer rejects,
- retain originals and provide end-to-end traceability from input to export.
