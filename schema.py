"""
Simple data model - just a dict, no classes needed
"""
from datetime import datetime


def normalize_product(raw_item):
    """
    Convert DummyJSON product to our simple format.
    
    Input: {"id": 1, "title": "iPhone", "price": 549}
    Output: {"id": "1", "title": "iPhone", "price": 549.0, "time": "2024-..."}
    """
    return {
        'id': str(raw_item['id']),
        'title': raw_item.get('title', 'Unknown'),
        'price': float(raw_item.get('price', 0)),
        'processed_at': datetime.utcnow().isoformat()
    }