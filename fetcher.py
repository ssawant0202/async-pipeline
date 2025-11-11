"""
Simple fetcher - just get products from DummyJSON API
"""
import asyncio
import httpx


async def fetch_products(queue):
    """
    Fetch products from DummyJSON and put them in queue.
    Gets first 3 pages only (30 products total).
    """
    print("🔄 Starting to fetch products...")
    
    async with httpx.AsyncClient() as client:
        for page in range(3):  # Just 3 pages
            try:
                # Calculate skip parameter (DummyJSON pagination)
                skip = page * 10
                url = f"https://dummyjson.com/products?limit=10&skip={skip}"
                
                print(f"  Fetching page {page + 1}...")
                
                # Get data
                response = await client.get(url, timeout=10.0)
                data = response.json()
                
                # Put each product in queue
                for product in data.get('products', []):
                    await queue.put(product)
                
                # Be nice to the API
                await asyncio.sleep(0.2)
                
            except Exception as e:
                print(f"  ❌ Error on page {page + 1}: {e}")
    
    print("✅ Fetching complete!")