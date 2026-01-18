# Tulip Tables Fivetran Connector

A Python-based Fivetran Source Connector to sync Tulip Table data into your data warehouse (Snowflake, BigQuery, Redshift, etc.).

## Overview

This connector enables automated data pipelines from Tulip Tables to your data warehouse with:

- 🔄 **Incremental sync** - Only syncs new/updated records with 60-second lookback
- 📊 **Dynamic schema discovery** - Automatically maps Tulip fields to warehouse columns
- 🎯 **Custom filtering** - Apply filters to sync only relevant records
- 🏢 **Workspace support** - Works with workspace-specific tables
- ⚡ **Efficient pagination** - Handles large datasets with automatic checkpointing
- 🛡️ **Production-ready** - Rate limiting, error handling, and retry logic

## Quick Start

### 1. Create Configuration

Create a `configuration.json` file (⚠️ never commit this file):

```json
{
  "subdomain": "yourcompany",
  "api_key": "your_api_key_here",
  "api_secret": "your_api_secret_here",
  "table_id": "T65jBaGMgiexWy5yS",
  "workspace_id": "n65jTTAeR8St6nX6k",
  "sync_from_date": "2025-01-01T00:00:00Z",
  "custom_filter_json": "[]"
}
```

### 2. Test Locally

```bash
# Install Fivetran SDK
pip install fivetran-connector-sdk

# Test the connector
fivetran debug --configuration configuration.json
```

### 3. Deploy to Fivetran

```bash
fivetran deploy \
  --api-key YOUR_FIVETRAN_API_KEY \
  --destination Snowflake \
  --connection tulip_table_sync
```

See [TULIP_INTEGRATION_GUIDE.md](TULIP_INTEGRATION_GUIDE.md) for complete setup instructions.

## Configuration Parameters

### Required

| Parameter | Description | Example |
|-----------|-------------|---------|
| `subdomain` | Your Tulip instance subdomain | `acme` |
| `api_key` | Tulip API key from Bot settings | `apikey.2_KWBF...` |
| `api_secret` | Tulip API secret from Bot settings | `WZq4kFmSi...` |
| `table_id` | UID of the Tulip table to sync | `T65jBaGMgiexWy5yS` |

### Optional

| Parameter | Description | Example | Default |
|-----------|-------------|---------|---------|
| `workspace_id` | Workspace ID for workspace-specific tables | `n65jTTAeR8St6nX6k` | `null` |
| `sync_from_date` | ISO-8601 timestamp for initial sync | `2025-01-01T00:00:00Z` | Start of time |
| `custom_filter_json` | JSON array of Tulip API filter objects | `[{"field":"status","functionType":"equal","arg":"active"}]` | `[]` |

## Features

### Schema Discovery

The connector automatically:
- Discovers all fields in your Tulip Table
- Maps Tulip data types to warehouse-compatible types
- Sets `id` as the primary key
- Includes system fields (`_createdAt`, `_updatedAt`, etc.)

**Type Mapping:**

| Tulip Type | Warehouse Type |
|------------|----------------|
| `integer` | `INT` |
| `float` | `DOUBLE` |
| `boolean` | `BOOLEAN` |
| `timestamp`, `datetime` | `UTC_DATETIME` |
| Others | `STRING` |

### Incremental Sync

- Tracks the last synced record using `_updatedAt` timestamp
- Applies 60-second lookback to ensure no records are missed
- Checkpoints every 500 records for resumable syncs

### Custom Filters

Apply Tulip API filters to sync only specific records:

```json
[
  {"field": "status", "functionType": "equal", "arg": "active"},
  {"field": "priority", "functionType": "greaterThan", "arg": 5}
]
```

Available filter functions: `equal`, `notEqual`, `greaterThan`, `lessThan`, `contains`, `startsWith`, `blank`, `isIn`, and more.

## Development

### Prerequisites

- Python 3.9+
- Virtual environment (recommended)
- Fivetran Connector SDK

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd tulip-fivetran-connector

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -r requirements-dev.txt
```

### Running Tests

```bash
# Run all tests
python -m pytest test_connector.py -v

# Run with coverage report
python -m pytest test_connector.py -v --cov=connector --cov-report=html

# Run specific test class
python -m pytest test_connector.py::TestSchemaDiscovery -v

# Run specific test
python -m pytest test_connector.py::TestUpdate::test_update_pagination -v
```

**Test Coverage: 100%** ✅

The test suite includes 15 tests covering:
- ✅ Schema discovery with/without workspace
- ✅ Initial and incremental syncs
- ✅ Pagination and checkpointing
- ✅ Rate limiting and error handling
- ✅ Custom filters and edge cases

### Local Testing with Real Data

```bash
# Create your configuration file
cp configuration.example.json configuration.json
# Edit configuration.json with your credentials

# Test the connector
fivetran debug --configuration configuration.json

# You should see output like:
# INFO: Fetching schema from https://yourcompany.tulip.co/api/v3/...
# INFO: Starting sync from cursor: 2025-01-01T00:00:00Z
# INFO: Fivetran-Tester-Process: SYNC SUCCEEDED
```

## Project Structure

```
tulip-fivetran-connector/
├── connector.py              # Main connector implementation
├── test_connector.py         # Comprehensive test suite
├── requirements.txt          # Production dependencies (empty - SDK provides all)
├── requirements-dev.txt      # Development dependencies (pytest, coverage)
├── README.md                 # This file
├── TULIP_INTEGRATION_GUIDE.md # Complete integration guide
├── configuration.json        # Your config (gitignored)
└── .gitignore               # Excludes sensitive files
```

## Deployment

### Deploy to Fivetran

1. **Get Fivetran API Key:**
   - Log into Fivetran
   - Navigate to Settings > API Config
   - Generate an API key

2. **Deploy the connector:**
   ```bash
   fivetran deploy \
     --api-key YOUR_FIVETRAN_API_KEY \
     --destination Snowflake \
     --connection tulip_table_sync
   ```

3. **Configure in Fivetran UI:**
   - Go to Connectors > Your Connector
   - Enter configuration parameters
   - Set sync schedule
   - Run initial sync

See [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk) for detailed deployment instructions.

## Monitoring

### Key Metrics

Monitor these metrics in the Fivetran dashboard:
- **Sync Success Rate** - Should be near 100%
- **Records Synced** - Compare with Tulip Table row count
- **Sync Duration** - Track performance over time
- **Data Freshness** - Check `_updatedAt` in your warehouse

### Logs

Enable debug logging in Fivetran:
1. Navigate to your connector
2. Go to Setup > Logs
3. Set log level to Debug
4. Run manual sync and review

## Troubleshooting

### Common Issues

**Authentication Error (401)**
- Verify API key and secret are correct
- Check bot has permissions to access the table
- Ensure bot is enabled in Tulip

**Table Not Found (404)**
- Verify table ID is correct
- If using workspace table, ensure workspace ID is provided
- Check table exists in your Tulip instance

**Rate Limiting (429)**
- Connector automatically retries after 5 seconds
- If persistent, reduce sync frequency or contact Tulip support

**Schema Changes**
- New fields are automatically detected on next sync
- New columns added to warehouse table
- Historical records have NULL for new fields

For more troubleshooting guidance, see [TULIP_INTEGRATION_GUIDE.md](TULIP_INTEGRATION_GUIDE.md#troubleshooting).

## Documentation

- **[Integration Guide](TULIP_INTEGRATION_GUIDE.md)** - Complete setup guide for Tulip + Fivetran + Snowflake
- **[Tulip API Docs](https://support.tulip.co/docs/api-documentation)** - Tulip API reference
- **[Tulip Table API](https://support.tulip.co/docs/table-api-guide)** - Table-specific API documentation
- **[Fivetran SDK Docs](https://fivetran.com/docs/connector-sdk)** - Fivetran Connector SDK reference

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass (100% coverage)
5. Submit a pull request

## Security

⚠️ **Never commit sensitive information:**
- Keep `configuration.json` out of version control (it's in `.gitignore`)
- Rotate API credentials regularly
- Use Fivetran's secrets management in production
- Follow least-privilege principles for bot permissions

## Support

- **Issues:** Open an issue in this repository
- **Tulip Community:** [community.tulip.co](https://community.tulip.co/)
- **Fivetran Support:** [support.fivetran.com](https://support.fivetran.com/)

## License

[Add your license here]

## Changelog

### v1.0.0 (January 2026)
- ✅ Initial release
- ✅ Schema discovery with dynamic type mapping
- ✅ Incremental sync with 60-second lookback
- ✅ Custom filter support
- ✅ Workspace-specific table support
- ✅ Automatic pagination and checkpointing (every 500 records)
- ✅ Rate limiting handling
- ✅ 100% test coverage with 15 tests
- ✅ Complete integration guide

## Roadmap / TODO

### Planned Enhancements

- [ ] **Fix column labels/IDs** - Use human-readable column names instead of field IDs
- [ ] **Fix table name/label/ID** - Use table label for warehouse table name instead of table ID
- [ ] **Multi-table version** - Support syncing multiple Tulip Tables in a single connector instance
- [ ] **Soft delete support** - Handle deleted records in Tulip Tables
- [ ] **Performance optimization** - Batch upserts for improved throughput
- [ ] **Advanced filtering UI** - Visual filter builder for easier configuration
- [ ] **Data validation** - Optional schema validation and data quality checks
- [ ] **Metrics tracking** - Built-in performance and data quality metrics

### Future Considerations

- Support for Tulip Analytics Records
- Change Data Capture (CDC) mode
- Bi-directional sync capabilities
- Custom transformation support

## Authors

[Add author information]

---

**Version:** 1.0.0
**Last Updated:** January 2026
**Compatibility:** Tulip API v3, Fivetran Connector SDK 2.0+
