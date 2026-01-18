

### 2. The Final Specification for the Engineer (Claude)

This is the document you can hand over to Claude (or any developer) to build the connector.

#### **Project: Tulip Tables Generic Fivetran Connector**

**Objective:** A Python-based Fivetran Source Connector to sync a specific Tulip Table into a destination.

**Configuration Inputs:**

1. `subdomain`: Tulip instance name (e.g., `acme`).
2. `api_key` & `api_secret`: Basic Auth credentials.
3. `table_id`: The UID of the Tulip table.
4. `workspace_id` (Optional): For workspace-specific API access.
5. `sync_from_date`: ISO-8601 string (e.g., `2023-01-01T00:00:00Z`) for the initial sync.
6. `custom_filter_json` (Optional): A JSON array of Tulip API filter objects.

**Core Logic Requirements:**

* **Endpoint Construction:** * If `workspace_id` exists: `https://{subdomain}.tulip.co/api/v3/w/{workspace_id}/tables/{table_id}/records`
* Else: `https://{subdomain}.tulip.co/api/v3/tables/{table_id}/records`


* **State Management:**
* Store the `updatedAt` value of the last record processed in the Fivetran `state`.
* Use a **60-second lookback** (subtract 60s from the state timestamp) on every request to ensure no records are missed.


* **Pagination & Checkpointing:**
* Fetch records in batches of 100.
* **Call `fivetran.checkpoint()` every 500 records.** This is critical to ensure that if the sync is interrupted, it resumes from the last successfully processed batch.


* **Schema Discovery:**
* Call the GET `/tables/{table_id}` endpoint.
* Iterate through the `fields` array to dynamically define the Fivetran schema.
* Map `id` as the **Primary Key**.


* **Incremental Filtering:**
* The request must always include a filter for `updatedAt > {state_timestamp}`.
* Combine this with any filters provided in `custom_filter_json`.

