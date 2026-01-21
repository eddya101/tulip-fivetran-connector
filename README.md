# Tulip Connector Example

This connector syncs data from Tulip Tables to Fivetran destinations. Tulip is a frontline operations platform that enables manufacturers to build apps without code, connect machines and devices, and analyze data to improve operations. This connector allows you to replicate Tulip Table data into your data warehouse for analysis and reporting.

## Connector overview

This connector provides incremental data replication from Tulip Tables using the Tulip Table API. It supports:

- Dynamic schema discovery that automatically maps Tulip field types to warehouse column types
- Incremental sync based on the `_updatedAt` timestamp field with a 60-second lookback window to prevent data loss
- Custom filtering using Tulip API filter syntax to sync only relevant records
- Workspace-scoped table access for multi-tenant Tulip instances
- Automatic pagination with checkpointing every 500 records for resumable syncs
- Exponential backoff retry logic for rate limiting and transient errors

The connector is designed for manufacturing operations teams who need to analyze production data, track quality metrics, and monitor operational performance across their data warehouse ecosystem.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Incremental sync: Uses `_updatedAt` timestamp field to sync only new or modified records
- Schema discovery: Automatically detects table schema and maps Tulip data types to Fivetran types
- Custom filtering: Supports Tulip API filter syntax for conditional data replication
- Workspace support: Can sync tables from workspace-scoped or instance-level endpoints
- Human-readable columns: Generates descriptive column names from field labels and IDs
- Checkpointing: Saves state every 500 records for resumable syncs
- Error handling: Implements exponential backoff retry logic for rate limiting and transient failures

## Configuration file

The connector requires the following configuration keys in `configuration.json`:

```json
{
  "subdomain": "yourcompany",
  "api_key": "apikey.2_KWBF636uT2fQEKjHX",
  "api_secret": "WZq4kFmSiPOHYv6czMmTGG__kjfAJGyn--fR-tZZkcm",
  "table_id": "T65jBaGMgiexWy5yS",
  "workspace_id": "n65jTTAeR8St6nX6k",
  "sync_from_date": "2025-01-01T00:00:00Z",
  "custom_filter_json": "[]"
}
```

Configuration parameters:

- `subdomain` (required) - Your Tulip instance subdomain (e.g., "acme" for acme.tulip.co)
- `api_key` (required) - Tulip API key obtained from Bot settings in Tulip
- `api_secret` (required) - Tulip API secret corresponding to the API key
- `table_id` (required) - Unique identifier of the Tulip table to sync (e.g., "T65jBaGMgiexWy5yS")
- `workspace_id` (optional) - Workspace ID for workspace-scoped tables (omit for instance-level tables)
- `sync_from_date` (optional) - ISO 8601 timestamp to start initial sync (defaults to beginning of time if omitted)
- `custom_filter_json` (optional) - JSON array of Tulip API filter objects (defaults to empty array if omitted)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector does not require any additional Python packages beyond those pre-installed in the Fivetran environment. The `requirements.txt` file should be left empty.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

Authentication - Refer to `def schema()` and `def update()`

This connector uses HTTP Basic Authentication with Tulip API credentials:

1. Log into your Tulip instance.
2. Navigate to **Shop Floor** > **Bots**.
3. Create a new bot or select an existing bot.
4. Copy the **API Key** and **API Secret** from the bot configuration.
5. Ensure the bot has appropriate permissions to access the target table.

The connector passes the API key and secret as HTTP Basic Auth credentials to all Tulip API endpoints.

## Pagination

Pagination - Refer to `def update()`

The connector implements offset-based pagination to handle large datasets:

- Fetches records in batches of 100 (configurable via `__DEFAULT_LIMIT` constant)
- Uses `offset` and `limit` query parameters in API requests
- Sorts results by `_updatedAt` timestamp in ascending order to ensure consistent ordering
- Continues pagination until fewer records than the limit are returned
- Checkpoints state every 500 records to enable resumable syncs

## Data handling

Data handling - Refer to `def schema()`, `def _map_tulip_type_to_fivetran()`, `def generate_column_name()`, and `def _transform_record()`

The connector transforms Tulip Table data through several stages:

1. Schema discovery:
   - Fetches table metadata from Tulip API
   - Maps Tulip data types to Fivetran types using `_map_tulip_type_to_fivetran()`
   - Generates human-readable column names from field labels and IDs
   - Includes system fields: `id` (primary key), `_createdAt`, `_updatedAt`

2. Type mapping:
   - `integer` maps to `INT`
   - `float` maps to `DOUBLE`
   - `boolean` maps to `BOOLEAN`
   - `timestamp` and `datetime` map to `UTC_DATETIME`
   - `interval` maps to `INT` (stored as seconds)
   - `user` maps to `STRING`
   - All other types default to `STRING`

3. Record transformation:
   - Transforms field IDs to human-readable column names using `generate_column_name()`
   - Column naming format: `label__id` (e.g., `customer_name__rqoqm`)
   - Normalizes labels: lowercase, replaces spaces with underscores, removes special characters
   - Preserves system fields in their original format

4. Incremental sync:
   - Filters records by `_updatedAt > last_cursor`
   - Applies 60-second lookback to prevent data loss from concurrent updates
   - Updates cursor after each record is processed

## Error handling

Error handling - Refer to `def _fetch_with_retry()`

The connector implements robust error handling:

- Rate limiting: Detects HTTP 429 responses and retries with exponential backoff (5s, 10s, 20s)
- Request failures: Automatically retries transient errors up to 3 attempts
- Specific exception handling: Catches and logs `KeyError`, `json.JSONDecodeError`, `requests.exceptions.HTTPError`
- Structured logging: Uses Python logging module with appropriate levels (INFO, WARNING, ERROR, CRITICAL)
- State preservation: Checkpoints every 500 records to minimize data loss on failure

## Tables created

The connector creates a single table per Tulip Table synced. The table name is generated from the Tulip table label and ID in the format `label__id`.

Example table schema for a Tulip table named "BioTechKitsDummy":

Table: `biotechkitsdummy__t65jbagmgiexwy5ys`

System columns (always present):
- `id` (STRING, primary key) - Unique record identifier
- `_createdAt` (UTC_DATETIME) - Record creation timestamp
- `_updatedAt` (UTC_DATETIME) - Record last update timestamp

Custom columns are generated based on the Tulip table schema and follow the naming pattern `fieldlabel__fieldid`. For example:
- `customer_name__rqoqm` (STRING)
- `kit_number__pxwol` (INT)
- `kit_start_datetime__rzkek_kit_start_date_time` (UTC_DATETIME)
- `spectrapath_data_success__pybts_spectra_path_data_success` (BOOLEAN)

The schema is automatically discovered on the first sync and updated when new fields are added to the Tulip table.

## Additional files

- `test_connector.py` - Comprehensive test suite with 15 tests covering schema discovery, incremental sync, pagination, error handling, and custom filters
- `DEVELOPMENT_GUIDE.md` - Development environment setup instructions and debugging workflow
- `TULIP_INTEGRATION_GUIDE.md` - End-to-end integration guide for setting up Tulip with Fivetran and Snowflake

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
