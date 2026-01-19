from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from datetime import datetime, timedelta
import requests
import json
import time
import sys
import re


def generate_column_name(field_id, field_label=None):
    """
    Generate Snowflake-friendly column name from Tulip field ID and label.

    Format: label__id (e.g., customer_name__rqoqm)

    Args:
        field_id: Tulip field ID (required)
        field_label: Tulip field label (optional, human-readable name)

    Returns:
        Snowflake-compatible column name
    """
    # Clean and normalize the label
    if field_label and field_label.strip():
        label = field_label.strip()

        # Convert to lowercase
        label = label.lower()

        # Replace spaces and hyphens with underscores
        label = label.replace(' ', '_').replace('-', '_')

        # Remove special characters (keep only alphanumeric and underscores)
        label = re.sub(r'[^a-z0-9_]', '', label)

        # Remove consecutive underscores
        label = re.sub(r'_+', '_', label)

        # Remove leading/trailing underscores
        label = label.strip('_')

        # If label starts with a number, prefix with 'field_'
        if label and label[0].isdigit():
            label = f"field_{label}"

        # If label is empty after cleaning, use field_id
        if not label:
            label = field_id.lower()
    else:
        # No label provided, use field_id
        label = field_id.lower()

    # Clean the field_id as well
    clean_id = field_id.lower()
    clean_id = re.sub(r'[^a-z0-9_]', '', clean_id)

    # Combine with double underscore: label__id
    column_name = f"{label}__{clean_id}"

    # Final safety check: ensure it's a valid identifier
    if not column_name or column_name[0].isdigit():
        column_name = f"field_{column_name}"

    return column_name


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

        # Add system fields first (these are always present in Tulip records)
        columns['id'] = 'STRING'
        columns['_createdAt'] = 'UTC_DATETIME'
        columns['_updatedAt'] = 'UTC_DATETIME'

        print(f"INFO: Discovered {len(table_metadata.get('columns', []))} fields")

        # Tulip fields mapping with human-readable names
        for field in table_metadata.get('columns', []):
            field_id = field['name']
            field_label = field.get('label', '')
            t_type = field['dataType']['type']

            # Skip system fields if they appear in the columns list
            if field_id in ['id', '_createdAt', '_updatedAt']:
                print(f"INFO: Skipping system field '{field_id}' from columns list")
                continue

            # Generate human-readable column name
            column_name = generate_column_name(field_id, field_label)

            # Map Tulip types to Fivetran types
            if t_type == 'integer':
                f_type = "INT"
            elif t_type == 'float':
                f_type = "DOUBLE"
            elif t_type == 'boolean':
                f_type = "BOOLEAN"
            elif t_type in ['timestamp', 'datetime']:
                f_type = "UTC_DATETIME"
            elif t_type == 'interval':
                # Interval type (duration/time periods) - store as integer seconds
                f_type = "INT"
            elif t_type == 'user':
                # User type (user references) - store as string
                f_type = "STRING"
            else:
                # Default fallback for any other types (text, string, etc.)
                f_type = "STRING"

            columns[column_name] = f_type
            print(f"INFO: Mapped field '{field_label}' ({field_id}) -> {column_name} ({f_type})")

        # Use table label if available, otherwise use table_id
        table_label = table_metadata.get('label', table_id)
        table_name = generate_column_name(table_id, table_label)

        print(f"INFO: Table name: {table_name} (from '{table_label}')")

        return [
            {
                "table": table_name,
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

        # First, get the schema to build field ID -> column name mapping
        if workspace_id:
            schema_url = f"https://{subdomain}.tulip.co/api/v3/w/{workspace_id}/tables/{table_id}"
        else:
            schema_url = f"https://{subdomain}.tulip.co/api/v3/tables/{table_id}"

        schema_response = requests.get(schema_url, auth=(api_key, api_secret))
        if schema_response.status_code != 200:
            raise Exception(f"Failed to fetch table schema: {schema_response.status_code}")

        table_metadata = schema_response.json()

        # Build mapping: field_id -> column_name
        field_mapping = {}
        for field in table_metadata.get('columns', []):
            field_id = field['name']
            # Skip system fields if they appear in the columns list
            if field_id in ['id', '_createdAt', '_updatedAt']:
                continue
            field_label = field.get('label', '')
            column_name = generate_column_name(field_id, field_label)
            field_mapping[field_id] = column_name

        # Get table name
        table_label = table_metadata.get('label', table_id)
        table_name = generate_column_name(table_id, table_label)
        
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
                # Transform record keys from field IDs to human-readable column names
                transformed_record = {}
                for field_id, value in record.items():
                    # Map field_id to column name
                    # For user fields, use the mapping; for system fields (starting with _), keep as-is
                    if field_id in field_mapping:
                        column_name = field_mapping[field_id]
                    else:
                        # System fields like _updatedAt, _createdAt, id should keep their original casing
                        column_name = field_id
                    transformed_record[column_name] = value

                # Use table_name instead of table_id
                op.upsert(table=table_name, data=transformed_record)
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