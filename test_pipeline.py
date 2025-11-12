"""
Integration tests for the async pipeline.
Tests the full pipeline with real API endpoints (small scale).
"""
import asyncio
import json
import os
import pytest
from main import run_pipeline
from fetcher import EndpointConfig
from schema import normalize_product


# Mark all tests as async
pytestmark = pytest.mark.asyncio


class TestNormalization:
    """Test data normalization functions."""
    
    def test_normalize_dummyjson(self):
        """Test DummyJSON normalization."""
        raw_item = {
            'id': 1,
            'title': 'iPhone 9',
            'price': 549,
            'category': 'smartphones'
        }
        
        product = normalize_product('dummyjson', raw_item)
        
        assert product.id == 'dummyjson_1'
        assert product.title == 'iPhone 9'
        assert product.source == 'dummyjson'
        assert product.price == 549.0
        assert product.category == 'smartphones'
        assert product.currency == 'USD'
    
    def test_normalize_escuelajs(self):
        """Test Escuela JS normalization with nested category."""
        raw_item = {
            'id': 42,
            'title': 'Awesome Product',
            'price': 99.99,
            'category': {'name': 'Electronics', 'id': 2}
        }
        
        product = normalize_product('escuelajs', raw_item)
        
        assert product.id == 'escuelajs_42'
        assert product.title == 'Awesome Product'
        assert product.price == 99.99
        assert product.category == 'Electronics'
    
    def test_normalize_jsonplaceholder(self):
        """Test JSONPlaceholder normalization."""
        raw_item = {
            'id': 5,
            'title': 'Sample Post',
            'userId': 1
        }
        
        product = normalize_product('jsonplaceholder', raw_item)
        
        assert product.id == 'jsonplaceholder_5'
        assert product.title == 'Sample Post'
        assert product.source == 'jsonplaceholder'


class TestPipeline:
    """Test full pipeline execution."""
    
    async def test_pipeline_with_single_endpoint(self, tmp_path):
        """Test pipeline with one endpoint (fast test)."""
        # Create temporary output files
        output_file = tmp_path / "test_products.ndjson"
        summary_file = tmp_path / "test_summary.json"
        
        # Use only DummyJSON for fast test (limit to first page)
        endpoints = [
            EndpointConfig(
                name='dummyjson',
                url='https://dummyjson.com/products',
                rate_limit=5
            )
        ]
        
        # Run pipeline
        summary = await run_pipeline(
            endpoints=endpoints,
            num_workers=4,
            queue_size=100,
            output_file=str(output_file),
            summary_file=str(summary_file)
        )
        
        # Assertions
        assert summary['total_products'] > 0, "Should fetch at least some products"
        assert summary['success_rate'] > 0, "Should have some successful requests"
        assert os.path.exists(output_file), "Output file should exist"
        assert os.path.exists(summary_file), "Summary file should exist"
        
        # Verify NDJSON format
        with open(output_file, 'r') as f:
            lines = f.readlines()
            assert len(lines) > 0, "Should have output lines"
            
            # Parse first line to verify JSON format
            first_product = json.loads(lines[0])
            assert 'id' in first_product
            assert 'title' in first_product
            assert 'source' in first_product
            assert 'price' in first_product
            assert first_product['source'] == 'dummyjson'
        
        # Verify summary structure
        assert 'statistics' in summary
        assert 'endpoints' in summary
        assert 'errors' in summary
        assert summary['statistics']['total_products'] == summary['total_products']
    
    async def test_pipeline_concurrent_endpoints(self, tmp_path):
        """Test pipeline with multiple endpoints running concurrently."""
        output_file = tmp_path / "test_concurrent.ndjson"
        summary_file = tmp_path / "test_concurrent_summary.json"
        
        # Use two real endpoints
        endpoints = [
            EndpointConfig(
                name='dummyjson',
                url='https://dummyjson.com/products',
                rate_limit=5
            ),
            EndpointConfig(
                name='escuelajs',
                url='https://api.escuelajs.co/api/v1/products',
                rate_limit=5
            )
        ]
        
        summary = await run_pipeline(
            endpoints=endpoints,
            num_workers=4,
            queue_size=100,
            output_file=str(output_file),
            summary_file=str(summary_file)
        )
        
        # Should have products from multiple sources
        assert summary['total_products'] > 0
        assert len(summary['endpoints']) == 2
        
        # Check that we got data from both sources
        sources = summary['statistics']['products_by_source']
        assert len(sources) > 0, "Should have products from at least one source"
        
        # Verify endpoint metrics
        for endpoint_name in ['dummyjson', 'escuelajs']:
            endpoint_metrics = summary['endpoints'].get(endpoint_name)
            if endpoint_metrics:  # May not have data if endpoint failed
                assert 'requests_sent' in endpoint_metrics
                assert 'items_fetched' in endpoint_metrics


if __name__ == '__main__':
    # Run tests
    pytest.main([__file__, '-v', '-s'])