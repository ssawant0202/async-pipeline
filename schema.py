
# Data models and normalization functions for the pipeline.

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Any


@dataclass
class Product:
    """Primary Schema - all sources map to this format."""
    id: str
    title: str
    source: str
    price: float
    currency: str
    category: str
    processed_at: str


def normalize_jsonplaceholder(raw_item: Dict[str, Any]) -> Product:
    """
    Map JSONPlaceholder API data to  schema.
    Example input: {"id": 1, "title": "Post Title", "userId": 1}
    """
    return Product(
        id=f"jsonplaceholder_{raw_item['id']}",
        title=raw_item.get('title', 'Unknown'),
        source='jsonplaceholder',
        price=9.99,  # Mock price since this API doesn't have products
        currency='USD',
        category='post',
        processed_at=datetime.utcnow().isoformat()
    )


def normalize_dummyjson(raw_item: Dict[str, Any]) -> Product:
    """
    Map DummyJSON API data to  schema.
    Example input: {"id": 1, "title": "iPhone 9", "price": 549, "category": "smartphones"}
    """
    return Product(
        id=f"dummyjson_{raw_item['id']}",
        title=raw_item.get('title', 'Unknown'),
        source='dummyjson',
        price=float(raw_item.get('price', 0)),
        currency='USD',
        category=raw_item.get('category', 'uncategorized'),
        processed_at=datetime.utcnow().isoformat()
    )


def normalize_escuelajs(raw_item: Dict[str, Any]) -> Product:
    """
    Map Escuela JS API data to schema.
    Example input: {"id": 1, "title": "Product", "price": 100, "category": {"name": "Clothes"}}
    """
    # Handle nested category structure
    category = raw_item.get('category', {})
    if isinstance(category, dict):
        category_name = category.get('name', 'uncategorized')
    else:
        category_name = str(category)
    
    return Product(
        id=f"escuelajs_{raw_item['id']}",
        title=raw_item.get('title', 'Unknown'),
        source='escuelajs',
        price=float(raw_item.get('price', 0)),
        currency='USD',
        category=category_name,
        processed_at=datetime.utcnow().isoformat()
    )


# Mapper registry - makes it easy to get the right normalizer
NORMALIZERS = {
    'jsonplaceholder': normalize_jsonplaceholder,
    'dummyjson': normalize_dummyjson,
    'escuelajs': normalize_escuelajs,
}


def normalize_product(source: str, raw_item: Dict[str, Any]) -> Product:

    # Returns: Normalized/Processed Product instance
  
    normalizer = NORMALIZERS.get(source)
    if not normalizer:
        raise ValueError(f"Unknown source: {source}")
    
    return normalizer(raw_item)