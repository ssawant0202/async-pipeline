"""
Simple pipeline - fetch products, process them, save results
"""
import asyncio
import json
from fetcher import fetch_products
from processor import process_products


async def main():
    """
    Main pipeline:
    1. Create a queue
    2. Start fetcher (puts items in queue)
    3. Start processor (takes items from queue)
    4. Wait for both to finish
    5. Save summary
    """
    print("=" * 50)
    print("🚀 Simple Async Pipeline Starting...")
    print("=" * 50)
    
    # Create queue to pass products from fetcher to processor
    queue = asyncio.Queue()
    
    # Start both tasks at the same time
    fetcher_task = asyncio.create_task(fetch_products(queue))
    processor_task = asyncio.create_task(
        process_products(queue, 'products.json')
    )
    
    # Wait for fetcher to finish
    await fetcher_task
    
    # Send stop signal to processor
    await queue.put(None)
    
    # Wait for processor to finish and get stats
    stats = await processor_task
    
    # Save summary
    with open('summary.json', 'w') as f:
        json.dump(stats, f, indent=2)
    
    print("=" * 50)
    print("✅ Pipeline Complete!")
    print(f"📄 Output: products.json")
    print(f"📊 Summary: summary.json")
    print("=" * 50)


if __name__ == '__main__':
    asyncio.run(main())