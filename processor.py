"""
Simple processor - normalize and save products
"""
import asyncio
import json
from schema import normalize_product


async def process_products(queue, output_file):
    """
    Get products from queue, normalize them, and save to file.
    Runs until it gets a None (stop signal).
    """
    print("🔄 Starting to process products...")
    
    processed_count = 0
    total_price = 0
    
    with open(output_file, 'w') as f:
        while True:
            # Get next product from queue
            raw_product = await queue.get()
            
            # Stop signal?
            if raw_product is None:
                queue.task_done()
                break
            
            try:
                # Normalize it
                product = normalize_product(raw_product)
                
                # Write to file (one JSON per line)
                f.write(json.dumps(product) + '\n')
                
                # Track stats
                processed_count += 1
                total_price += product['price']
                
            except Exception as e:
                print(f"  ❌ Error processing product: {e}")
            
            finally:
                queue.task_done()
    
    # Calculate average price
    avg_price = total_price / processed_count if processed_count > 0 else 0
    
    print(f"✅ Processing complete!")
    print(f"  Total products: {processed_count}")
    print(f"  Average price: ${avg_price:.2f}")
    
    return {
        'total_products': processed_count,
        'average_price': round(avg_price, 2)
    }