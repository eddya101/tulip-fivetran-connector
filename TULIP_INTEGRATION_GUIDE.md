# Fivetran & Snowflake Integration Guide

## Overview

This guide demonstrates how to sync Tulip Table data to Snowflake using a custom Fivetran connector. This integration enables you to:

- **Automate data pipelines** - Schedule automatic syncs of Tulip Table data to Snowflake
- **Centralize analytics** - Combine Tulip manufacturing data with other business data in Snowflake
- **Enable real-time insights** - Incremental sync with 60-second lookback ensures near real-time data availability
- **Scale effortlessly** - Handle tables of any size with automatic pagination and checkpointing

## Prerequisites

Before you begin, ensure you have:

- ✅ A Tulip account with API access
- ✅ Tulip API credentials (API Key and API Secret)
- ✅ A Fivetran account ([sign up here](https://fivetran.com/signup))
- ✅ A Snowflake account with appropriate permissions
- ✅ The Table ID of the Tulip Table you want to sync
- ✅ (Optional) Workspace ID if using workspace-specific tables

## Architecture

```
┌─────────────┐         ┌──────────────┐         ┌─────────────┐
│             │         │              │         │             │
│  Tulip      │────────▶│  Fivetran    │────────▶│  Snowflake  │
│  Tables     │   API   │  Connector   │   SQL   │  Warehouse  │
│             │         │              │         │             │
└─────────────┘         └──────────────┘         └─────────────┘
```

The connector:
1. **Discovers schema** - Automatically maps Tulip Table fields to Snowflake columns
2. **Syncs data** - Pulls records from Tulip Tables API with incremental updates
3. **Loads to Snowflake** - Upserts data into your Snowflake warehouse

## Part 1: Gather Tulip Credentials

### Step 1: Obtain API Credentials

1. Log into your Tulip instance
2. Navigate to **Settings** > **Bots**
3. Click **Create Bot** or select an existing bot
4. Copy the **API Key** and **API Secret**

> **Note:** Keep these credentials secure. They provide access to your Tulip data.

### Step 2: Find Your Table ID

You can find your Table ID in multiple ways:

**Method 1: From the URL**
When viewing a table in Tulip, the URL contains the Table ID:
```
https://yourcompany.tulip.co/tables/T65jBaGMgiexWy5yS
                                      ↑
                                  Table ID
```

**Method 2: Using the API**
Call the Tables API endpoint:
```bash
curl -X GET "https://yourcompany.tulip.co/api/v3/tables" \
  -u "your_api_key:your_api_secret"
```

### Step 3: Find Your Workspace ID (Optional)

If your table is workspace-specific, you'll need the Workspace ID.

From the URL when viewing a workspace:
```
https://yourcompany.tulip.co/w/n65jTTAeR8St6nX6k/tables
                                ↑
                          Workspace ID
```

## Part 2: Deploy the Fivetran Connector

### Step 1: Install Fivetran CLI

```bash
# Install the Fivetran Connector SDK
pip install fivetran-connector-sdk

# Verify installation
fivetran --version
```

For detailed installation instructions, see [Fivetran Connector SDK documentation](https://fivetran.com/docs/connector-sdk).

### Step 2: Download the Connector Code

Create a new directory for your connector:

```bash
mkdir tulip-fivetran-connector
cd tulip-fivetran-connector
```

Create the following files:

#### `connector.py`

```python
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from datetime import datetime, timedelta
import requests
import json
import time
import sys

def schema(configuration):
    try:
        subdomain = configuration['subdomain']
        api_key = configuration['api_key']
        api_secret = configuration['api_secret']
        table_id = configuration['table_id']
        workspace_id = configuration.get('workspace_id')

        # Construct Base URL
        if workspace_id:
            url = f"https://{subdomain}.tulip.co/api/v3/w/{workspace_id}/tables/{table_id}"
        else:
            url = f"https://{subdomain}.tulip.co/api/v3/tables/{table_id}"

        print(f"INFO: Fetching schema from {url}")

        response = requests.get(url, auth=(api_key, api_secret))

        if response.status_code != 200:
            print(f"ERROR: Tulip API returned {response.status_code}: {response.text}")
            response.raise_for_status()

        table_metadata = response.json()
        columns = {}

        # Tulip fields mapping
        for field in table_metadata.get('fields', []):
            name = field['id']
            t_type = field['type']

            if t_type == 'integer':
                f_type = "INT"
            elif t_type == 'float':
                f_type = "DOUBLE"
            elif t_type == 'boolean':
                f_type = "BOOLEAN"
            elif t_type in ['timestamp', 'datetime']:
                f_type = "UTC_DATETIME"
            else:
                f_type = "STRING"

            columns[name] = f_type

        return [
            {
                "table": table_id,
                "primary_key": ["id"],
                "columns": columns
            }
        ]

    except Exception as e:
        print(f"CRITICAL: Schema discovery failed: {str(e)}")
        raise e

def update(configuration, state):
    try:
        subdomain = configuration['subdomain']
        api_key = configuration['api_key']
        api_secret = configuration['api_secret']
        table_id = configuration['table_id']
        workspace_id = configuration.get('workspace_id')
        sync_from = configuration.get('sync_from_date')

        # Robust JSON parsing
        raw_filter = configuration.get('custom_filter_json')
        custom_filters = json.loads(raw_filter) if raw_filter and raw_filter.strip() else []

        cursor = state.get('last_updated_at') or sync_from

        if cursor:
            # Handle Z suffix for datetime parsing
            clean_cursor = cursor.replace("Z", "+00:00")
            cursor_dt = datetime.fromisoformat(clean_cursor)
            cursor_dt = cursor_dt - timedelta(seconds=60)
            cursor = cursor_dt.isoformat().replace("+00:00", "Z")

        if workspace_id:
            url = f"https://{subdomain}.tulip.co/api/v3/w/{workspace_id}/tables/{table_id}/records"
        else:
            url = f"https://{subdomain}.tulip.co/api/v3/tables/{table_id}/records"

        print(f"INFO: Starting sync from cursor: {cursor}")

        limit = 100
        offset = 0
        has_more = True
        records_processed = 0

        while has_more:
            api_filters = [{"field": "_updatedAt", "functionType": "greaterThan", "arg": cursor}] if cursor else []
            api_filters.extend(custom_filters)

            params = {
                'limit': limit,
                'offset': offset,
                'filters': json.dumps(api_filters),
                'sortOptions': json.dumps([{"sortBy": "_updatedAt", "sortDir": "asc"}])
            }

            response = requests.get(url, auth=(api_key, api_secret), params=params)

            if response.status_code == 429:
                print("WARNING: Rate limited. Retrying...")
                time.sleep(5)
                continue

            if response.status_code != 200:
                print(f"ERROR: Tulip API returned {response.status_code}")
                print(f"ERROR: Response body: {response.text}")
                print(f"ERROR: Request URL: {response.url}")
                response.raise_for_status()
            records = response.json()

            if not records:
                break

            for record in records:
                op.upsert(table=table_id, data=record)
                cursor = record.get('_updatedAt') or record.get('updatedAt')
                records_processed += 1

                # Checkpoint every 500 records as per spec
                if records_processed % 500 == 0:
                    op.checkpoint(state={'last_updated_at': cursor})
                    print(f"INFO: Checkpointed at {records_processed} records")

            if len(records) < limit:
                has_more = False
            else:
                offset += limit

        # Final checkpoint to save the last cursor position
        if cursor:
            op.checkpoint(state={'last_updated_at': cursor})

    except Exception as e:
        print(f"CRITICAL: Update failed: {str(e)}")
        raise e

connector = Connector(schema=schema, update=update)
```

#### `requirements.txt`

```txt
# All dependencies are provided by the Fivetran platform
# Only add third-party libraries that are not already available
```

#### `configuration.json`

Create a configuration file for local testing:

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

> ⚠️ **Security Warning:** Never commit `configuration.json` to version control. Add it to your `.gitignore` file.

### Step 3: Test Locally

Before deploying to Fivetran, test the connector locally:

```bash
# Test the connector with your configuration
fivetran debug --configuration configuration.json
```

You should see output similar to:

```
INFO: Fetching schema from https://yourcompany.tulip.co/api/v3/w/.../tables/...
INFO: Starting sync from cursor: 2025-01-01T00:00:00Z
INFO: Fivetran-Tester-Process: SYNC SUCCEEDED
```

### Step 4: Deploy to Fivetran

Deploy the connector using the Fivetran CLI:

```bash
fivetran deploy \
  --api-key YOUR_FIVETRAN_API_KEY \
  --destination Snowflake \
  --connection tulip_table_sync
```

For detailed deployment instructions, see:
- [Fivetran Connector SDK - Deployment Guide](https://fivetran.com/docs/connector-sdk/working-with-connector-sdk)
- [Fivetran API Authentication](https://fivetran.com/docs/rest-api/getting-started)

## Part 3: Configure Fivetran Connection

### Step 1: Set Up Snowflake Destination

If you haven't already connected Snowflake to Fivetran:

1. Log into your Fivetran account
2. Navigate to **Destinations** > **Add Destination**
3. Select **Snowflake**
4. Enter your Snowflake credentials:
   - Account name
   - Database
   - Warehouse
   - Username and Password (or Key Pair)
5. Click **Test Connection** and **Save**

See [Fivetran Snowflake Setup Guide](https://fivetran.com/docs/destinations/snowflake/setup-guide) for detailed instructions.

### Step 2: Create Connector Connection

1. In Fivetran, navigate to **Connectors** > **Add Connector**
2. Search for your deployed "Tulip Tables" connector
3. Enter the configuration parameters:

| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `subdomain` | Your Tulip instance subdomain | `acme` | ✅ |
| `api_key` | Tulip API Key | `apikey.2_KWBF...` | ✅ |
| `api_secret` | Tulip API Secret | `WZq4kFmSi...` | ✅ |
| `table_id` | Tulip Table ID | `T65jBaGMgiexWy5yS` | ✅ |
| `workspace_id` | Workspace ID (if applicable) | `n65jTTAeR8St6nX6k` | ❌ |
| `sync_from_date` | Initial sync start date | `2025-01-01T00:00:00Z` | ❌ |
| `custom_filter_json` | Additional filters (JSON array) | `[]` | ❌ |

4. Select your Snowflake destination
5. Configure sync schedule (e.g., every 6 hours)
6. Click **Save & Test**

### Step 3: Verify Initial Sync

1. Monitor the initial sync in the Fivetran dashboard
2. Check the sync logs for any errors
3. Once complete, verify data in Snowflake:

```sql
-- View your synced Tulip Table data
SELECT * FROM your_schema.t_65_j_ba_gmgiex_wy_5_y_s LIMIT 10;

-- Check record count
SELECT COUNT(*) FROM your_schema.t_65_j_ba_gmgiex_wy_5_y_s;

-- View most recently updated records
SELECT * FROM your_schema.t_65_j_ba_gmgiex_wy_5_y_s
ORDER BY _updatedAt DESC
LIMIT 10;
```

> **Note:** Fivetran will normalize the table name (e.g., `T65jBaGMgiexWy5yS` becomes `t_65_j_ba_gmgiex_wy_5_y_s`)

## Part 4: Advanced Configuration

### Custom Filters

You can apply custom filters to limit which records are synced. Use Tulip's filter syntax:

```json
[
  {
    "field": "status",
    "functionType": "equal",
    "arg": "active"
  },
  {
    "field": "priority",
    "functionType": "greaterThan",
    "arg": 5
  }
]
```

Available filter functions:
- `equal`, `notEqual`
- `greaterThan`, `greaterThanOrEqual`
- `lessThan`, `lessThanOrEqual`
- `contains`, `notContains`
- `startsWith`, `endsWith`
- `blank`, `notBlank`
- `isIn`, `notIsIn`

See [Tulip API - Set Query Parameters](https://support.tulip.co/docs/set-query-parameters) for details.

### Sync Schedule

Configure your sync frequency based on your needs:

- **Real-time (< 5 min):** For critical operational data
- **Hourly:** For dashboards and reporting
- **Daily:** For analytics and batch processing

The connector uses incremental sync with a 60-second lookback to ensure no records are missed.

### Multiple Tables

To sync multiple Tulip Tables:

1. Deploy separate connector instances for each table
2. Or modify the connector to accept multiple table IDs
3. Each table will create a separate table in Snowflake

## Part 5: Data Model in Snowflake

### Schema Mapping

Tulip field types are automatically mapped to Snowflake data types:

| Tulip Type | Snowflake Type |
|------------|----------------|
| `integer` | `NUMBER(38,0)` |
| `float` | `FLOAT` |
| `boolean` | `BOOLEAN` |
| `timestamp` | `TIMESTAMP_TZ` |
| `datetime` | `TIMESTAMP_TZ` |
| `text` / other | `VARCHAR` |

### System Fields

The connector includes Tulip's system fields:

- `id` - Record ID (primary key)
- `_createdAt` - Record creation timestamp
- `_updatedAt` - Last update timestamp
- `_sequenceNumber` - Record sequence number

### Example Queries

**Get records updated today:**
```sql
SELECT *
FROM your_schema.t_65_j_ba_gmgiex_wy_5_y_s
WHERE DATE(_updatedAt) = CURRENT_DATE;
```

**Join with other data:**
```sql
SELECT
  t.id,
  t.product_name,
  t.quantity,
  o.order_number,
  o.customer_name
FROM your_schema.t_65_j_ba_gmgiex_wy_5_y_s t
LEFT JOIN your_schema.orders o ON t.order_id = o.id;
```

**Aggregate metrics:**
```sql
SELECT
  DATE(_createdAt) as date,
  COUNT(*) as records_created,
  AVG(cycle_time) as avg_cycle_time
FROM your_schema.t_65_j_ba_gmgiex_wy_5_y_s
GROUP BY DATE(_createdAt)
ORDER BY date DESC;
```

## Troubleshooting

### Common Issues

**1. Authentication Failed**

**Error:** `401 Unauthorized`

**Solution:**
- Verify your API Key and API Secret are correct
- Ensure the bot has permissions to access the table
- Check if the bot is enabled in Tulip

**2. Table Not Found**

**Error:** `404 Not Found`

**Solution:**
- Verify the Table ID is correct
- If using a workspace table, ensure the Workspace ID is provided
- Check that the table exists in your Tulip instance

**3. Rate Limiting**

**Error:** `429 Too Many Requests`

**Solution:**
- The connector automatically retries after 5 seconds
- If persistent, reduce sync frequency
- Contact Tulip support to increase rate limits

**4. Schema Changes**

If you add fields to your Tulip Table:
- Fivetran will automatically detect the new fields on the next sync
- New columns will be added to your Snowflake table
- Historical records will have NULL values for new fields

### Debugging

Enable verbose logging in Fivetran:
1. Navigate to your connector in Fivetran
2. Go to **Setup** > **Logs**
3. Set log level to **Debug**
4. Run a manual sync and review logs

For local debugging:
```bash
fivetran debug --configuration configuration.json --log-level DEBUG
```

## Monitoring & Maintenance

### Key Metrics to Monitor

1. **Sync Success Rate** - Track failed syncs in Fivetran dashboard
2. **Data Freshness** - Monitor `_updatedAt` timestamps in Snowflake
3. **Record Count** - Compare Tulip Table count vs Snowflake
4. **Sync Duration** - Track how long syncs take as data grows

### Maintenance Tasks

**Monthly:**
- Review sync logs for any warnings or errors
- Verify data quality and completeness
- Check Snowflake storage usage

**Quarterly:**
- Review and optimize sync schedule
- Update connector if new features are available
- Audit API credentials and rotate if needed

**Annually:**
- Review custom filters and configurations
- Consider archiving old data
- Evaluate performance and scaling needs

## Best Practices

### Security

- ✅ Use dedicated bot accounts for API access
- ✅ Rotate API credentials regularly
- ✅ Use Fivetran's built-in secrets management
- ✅ Apply least-privilege permissions in Snowflake
- ✅ Enable audit logging in both Tulip and Snowflake

### Performance

- ✅ Set sync frequency based on your data freshness needs
- ✅ Use custom filters to reduce data volume
- ✅ Monitor Snowflake compute usage
- ✅ Consider partitioning large tables by date

### Data Quality

- ✅ Validate data after initial sync
- ✅ Set up data quality checks in Snowflake
- ✅ Monitor for schema changes in Tulip Tables
- ✅ Document your data model and transformations

## Additional Resources

### Tulip Documentation
- [Tulip API Documentation](https://support.tulip.co/docs/api-documentation)
- [Table API Guide](https://support.tulip.co/docs/table-api-guide)
- [Set Query Parameters](https://support.tulip.co/docs/set-query-parameters)

### Fivetran Documentation
- [Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Snowflake Destination](https://fivetran.com/docs/destinations/snowflake)
- [API Reference](https://fivetran.com/docs/rest-api)

### Community & Support
- [Tulip Community Forum](https://community.tulip.co/)
- [Fivetran Support](https://support.fivetran.com/)

## Conclusion

You now have a fully functional data pipeline from Tulip Tables to Snowflake! This integration enables:

- **Automated data flows** from Tulip to your data warehouse
- **Near real-time analytics** with incremental sync
- **Scalable architecture** that grows with your data
- **Centralized data** for comprehensive business intelligence

For questions or issues, contact your Tulip Customer Success Manager or reach out in the [Tulip Community](https://community.tulip.co/).

---

**Last Updated:** January 2026
**Connector Version:** 1.0.0
**Compatibility:** Tulip API v3, Fivetran Connector SDK 2.0+
