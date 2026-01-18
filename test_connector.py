"""
Test suite for Tulip Tables Fivetran Connector

Run tests with: python -m pytest test_connector.py -v
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import json
from connector import schema, update


class TestSchemaDiscovery:
    """Tests for the schema() function"""

    @patch('connector.requests.get')
    def test_schema_success_with_workspace(self, mock_get):
        """Test successful schema discovery with workspace_id"""
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'fields': [
                {'id': 'name', 'type': 'text'},
                {'id': 'age', 'type': 'integer'},
                {'id': 'salary', 'type': 'float'},
                {'id': 'is_active', 'type': 'boolean'},
                {'id': 'created_at', 'type': 'timestamp'},
            ]
        }
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'workspace_id': 'W456'
        }

        result = schema(config)

        # Verify URL construction
        mock_get.assert_called_once_with(
            'https://test.tulip.co/api/v3/w/W456/tables/T123',
            auth=('key', 'secret')
        )

        # Verify schema structure
        assert len(result) == 1
        assert result[0]['table'] == 'T123'
        assert result[0]['primary_key'] == ['id']
        assert 'columns' in result[0]

        # Verify column types
        columns = result[0]['columns']
        assert columns['name'] == 'STRING'
        assert columns['age'] == 'INT'
        assert columns['salary'] == 'DOUBLE'
        assert columns['is_active'] == 'BOOLEAN'
        assert columns['created_at'] == 'UTC_DATETIME'

    @patch('connector.requests.get')
    def test_schema_success_without_workspace(self, mock_get):
        """Test successful schema discovery without workspace_id"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'fields': []}
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123'
        }

        schema(config)

        # Verify URL construction without workspace
        mock_get.assert_called_once_with(
            'https://test.tulip.co/api/v3/tables/T123',
            auth=('key', 'secret')
        )

    @patch('connector.requests.get')
    def test_schema_api_error(self, mock_get):
        """Test schema discovery handles API errors"""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = 'Unauthorized'
        mock_response.raise_for_status.side_effect = Exception('401 Unauthorized')
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'bad_key',
            'api_secret': 'bad_secret',
            'table_id': 'T123'
        }

        with pytest.raises(Exception):
            schema(config)

    @patch('connector.requests.get')
    def test_schema_unknown_field_type(self, mock_get):
        """Test that unknown field types default to STRING"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'fields': [
                {'id': 'custom_field', 'type': 'unknown_type'},
            ]
        }
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123'
        }

        result = schema(config)
        assert result[0]['columns']['custom_field'] == 'STRING'


class TestUpdate:
    """Tests for the update() function"""

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_update_initial_sync(self, mock_get, mock_op):
        """Test initial sync with no prior state"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {'id': '1', '_updatedAt': '2026-01-18T10:00:00Z', 'name': 'Test 1'},
            {'id': '2', '_updatedAt': '2026-01-18T11:00:00Z', 'name': 'Test 2'},
        ]
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'workspace_id': 'W456',
            'sync_from_date': '2026-01-01T00:00:00Z',
            'custom_filter_json': '[]'
        }
        state = {}

        update(config, state)

        # Verify upsert was called for each record
        assert mock_op.upsert.call_count == 2
        mock_op.upsert.assert_any_call(
            table='T123',
            data={'id': '1', '_updatedAt': '2026-01-18T10:00:00Z', 'name': 'Test 1'}
        )
        mock_op.upsert.assert_any_call(
            table='T123',
            data={'id': '2', '_updatedAt': '2026-01-18T11:00:00Z', 'name': 'Test 2'}
        )

        # Verify checkpoint was called
        assert mock_op.checkpoint.called

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_update_incremental_sync(self, mock_get, mock_op):
        """Test incremental sync with existing state"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {'id': '3', '_updatedAt': '2026-01-18T12:00:00Z', 'name': 'Test 3'},
        ]
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'workspace_id': 'W456',
            'custom_filter_json': '[]'
        }
        state = {'last_updated_at': '2026-01-18T11:00:00Z'}

        update(config, state)

        # Verify the filter includes the cursor with 60-second lookback
        call_args = mock_get.call_args
        params = call_args.kwargs['params']
        filters = json.loads(params['filters'])

        # Check that filter has _updatedAt field
        assert filters[0]['field'] == '_updatedAt'
        assert filters[0]['functionType'] == 'greaterThan'

        # Verify 60-second lookback was applied
        cursor_time = datetime.fromisoformat(filters[0]['arg'].replace('Z', '+00:00'))
        expected_time = datetime.fromisoformat('2026-01-18T11:00:00+00:00') - timedelta(seconds=60)
        assert cursor_time == expected_time

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_update_with_custom_filters(self, mock_get, mock_op):
        """Test sync with custom filters"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        custom_filter = [{"field": "status", "functionType": "equal", "arg": "active"}]
        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'sync_from_date': '2026-01-01T00:00:00Z',
            'custom_filter_json': json.dumps(custom_filter)
        }
        state = {}

        update(config, state)

        # Verify custom filter was included
        call_args = mock_get.call_args
        params = call_args.kwargs['params']
        filters = json.loads(params['filters'])

        # Should have both _updatedAt filter and custom filter
        assert len(filters) == 2
        assert filters[1] == custom_filter[0]

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_update_pagination(self, mock_get, mock_op):
        """Test that pagination works correctly"""
        # First page: 100 records
        first_page = [{'id': str(i), '_updatedAt': '2026-01-18T10:00:00Z'} for i in range(100)]
        # Second page: 50 records (less than limit, should stop)
        second_page = [{'id': str(i), '_updatedAt': '2026-01-18T10:00:00Z'} for i in range(100, 150)]

        mock_response_1 = Mock()
        mock_response_1.status_code = 200
        mock_response_1.json.return_value = first_page

        mock_response_2 = Mock()
        mock_response_2.status_code = 200
        mock_response_2.json.return_value = second_page

        mock_get.side_effect = [mock_response_1, mock_response_2]

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'sync_from_date': '2026-01-01T00:00:00Z',
            'custom_filter_json': '[]'
        }
        state = {}

        update(config, state)

        # Verify two API calls were made (pagination)
        assert mock_get.call_count == 2

        # Verify offset increased
        first_call_params = mock_get.call_args_list[0].kwargs['params']
        second_call_params = mock_get.call_args_list[1].kwargs['params']
        assert first_call_params['offset'] == 0
        assert second_call_params['offset'] == 100

        # Verify all records were upserted
        assert mock_op.upsert.call_count == 150

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_update_checkpoint_frequency(self, mock_get, mock_op):
        """Test that checkpoints occur every 500 records"""
        # Create 550 records to test checkpointing at 500
        records = [{'id': str(i), '_updatedAt': '2026-01-18T10:00:00Z'} for i in range(550)]

        # Split into batches of 100
        mock_responses = []
        for i in range(0, 550, 100):
            batch = records[i:i+100]
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = batch
            mock_responses.append(mock_response)

        mock_get.side_effect = mock_responses

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'sync_from_date': '2026-01-01T00:00:00Z',
            'custom_filter_json': '[]'
        }
        state = {}

        update(config, state)

        # Should checkpoint at 500 records + final checkpoint
        # Total checkpoint calls should be 2 (at 500 and final)
        assert mock_op.checkpoint.call_count == 2

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_update_rate_limiting(self, mock_get, mock_op):
        """Test that rate limiting is handled with retry"""
        mock_rate_limited = Mock()
        mock_rate_limited.status_code = 429

        mock_success = Mock()
        mock_success.status_code = 200
        mock_success.json.return_value = [
            {'id': '1', '_updatedAt': '2026-01-18T10:00:00Z'}
        ]

        # First call rate limited, second succeeds
        mock_get.side_effect = [mock_rate_limited, mock_success]

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'sync_from_date': '2026-01-01T00:00:00Z',
            'custom_filter_json': '[]'
        }
        state = {}

        with patch('connector.time.sleep') as mock_sleep:
            update(config, state)
            # Verify retry logic was triggered
            mock_sleep.assert_called_once_with(5)

        # Should have retried and succeeded
        assert mock_get.call_count == 2
        assert mock_op.upsert.call_count == 1

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_update_api_error(self, mock_get, mock_op):
        """Test that API errors are properly raised"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_response.url = 'https://test.tulip.co/api/v3/tables/T123/records'
        mock_response.raise_for_status.side_effect = Exception('500 Server Error')
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'sync_from_date': '2026-01-01T00:00:00Z',
            'custom_filter_json': '[]'
        }
        state = {}

        with pytest.raises(Exception):
            update(config, state)

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_update_empty_response(self, mock_get, mock_op):
        """Test handling of empty API response"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'sync_from_date': '2026-01-01T00:00:00Z',
            'custom_filter_json': '[]'
        }
        state = {}

        update(config, state)

        # No upserts should occur
        assert mock_op.upsert.call_count == 0

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_update_sort_options(self, mock_get, mock_op):
        """Test that sortOptions are correctly formatted"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'sync_from_date': '2026-01-01T00:00:00Z',
            'custom_filter_json': '[]'
        }
        state = {}

        update(config, state)

        # Verify sortOptions format
        call_args = mock_get.call_args
        params = call_args.kwargs['params']
        sort_options = json.loads(params['sortOptions'])

        assert len(sort_options) == 1
        assert sort_options[0]['sortBy'] == '_updatedAt'
        assert sort_options[0]['sortDir'] == 'asc'


class TestConfiguration:
    """Tests for configuration validation"""

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_missing_workspace_id(self, mock_get, mock_op):
        """Test that connector works without workspace_id"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'sync_from_date': '2026-01-01T00:00:00Z'
        }
        state = {}

        update(config, state)

        # Verify URL without workspace
        call_args = mock_get.call_args
        url = call_args.args[0]
        assert '/w/' not in url
        assert url == 'https://test.tulip.co/api/v3/tables/T123/records'

    @patch('connector.op')
    @patch('connector.requests.get')
    def test_empty_custom_filter(self, mock_get, mock_op):
        """Test that empty custom_filter_json is handled"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        config = {
            'subdomain': 'test',
            'api_key': 'key',
            'api_secret': 'secret',
            'table_id': 'T123',
            'sync_from_date': '2026-01-01T00:00:00Z',
            'custom_filter_json': ''
        }
        state = {}

        update(config, state)

        # Should not raise an error
        assert mock_get.called


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
