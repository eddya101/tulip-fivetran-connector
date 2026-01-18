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