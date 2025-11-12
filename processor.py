"""
Worker pool for processing fetched items.
Workers normalize data, update aggregates, and stream to output file.
"""
import asyncio
import json
import logging
from typing import Dict, Any
from collections import defaultdict
from dataclasses import asdict
from schema import normalize_product

logger = logging.getLogger(__name__)


class Aggregates:
    """
    Track aggregate statistics during processing.
    This is simpler than using locks everywhere.
    """
    def __init__(self):
        self.total_products = 0
        self.failed_normalizations = 0
        self.category_counts = defaultdict(int)
        self.price_sum_by_category = defaultdict(float)
        self.price_count_by_category = defaultdict(int)
        self.source_counts = defaultdict(int)
    
    def add_product(self, product):
        """Update aggregates with a new product."""
        self.total_products += 1
        self.category_counts[product.category] += 1
        self.source_counts[product.source] += 1
        
        if product.price > 0:
            self.price_sum_by_category[product.category] += product.price
            self.price_count_by_category[product.category] += 1
    
    def add_failure(self):
        """Record a normalization failure."""
        self.failed_normalizations += 1
    
    def get_average_prices(self) -> Dict[str, float]:
        """Calculate average price per category."""
        averages = {}
        for category in self.price_sum_by_category:
            count = self.price_count_by_category[category]
            if count > 0:
                averages[category] = round(
                    self.price_sum_by_category[category] / count, 
                    2
                )
        return averages
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert aggregates to dictionary for JSON output."""
        return {
            'total_products': self.total_products,
            'failed_normalizations': self.failed_normalizations,
            'products_by_category': dict(self.category_counts),
            'products_by_source': dict(self.source_counts),
            'average_price_by_category': self.get_average_prices()
        }


async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    aggregates: Aggregates,
    output_file_path: str
) -> None:
    """
    Process items from the queue.
    
    Each worker:
    1. Gets raw items from queue
    2. Normalizes them to canonical schema
    3. Updates shared aggregates
    4. Writes normalized item to NDJSON file
    
    Args:
        worker_id: Worker number (for logging)
        queue: Queue to get items from
        aggregates: Shared aggregates object
        output_file_path: Path to NDJSON output file
    """
    logger.info(f"Worker {worker_id} started")
    processed = 0
    
    # Each worker opens its own file handle (append mode is thread-safe for writes)
    with open(output_file_path, 'a') as f:
        while True:
            try:
                # Get item from queue (wait if empty)
                item = await queue.get()
                
                # Check for poison pill (shutdown signal)
                if item is None:
                    queue.task_done()
                    break
                
                # Normalize the item
                try:
                    source = item['source']
                    raw_payload = item['payload']
                    
                    product = normalize_product(source, raw_payload)
                    
                    # Update aggregates
                    aggregates.add_product(product)
                    
                    # Write to NDJSON file (one JSON object per line)
                    json_line = json.dumps(asdict(product))
                    f.write(json_line + '\n')
                    
                    processed += 1
                    
                    if processed % 100 == 0:
                        logger.debug(f"Worker {worker_id}: Processed {processed} items")
                
                except Exception as e:
                    aggregates.add_failure()
                    logger.error(f"Worker {worker_id}: Normalization error: {str(e)}")
                
                finally:
                    # Mark task as done
                    queue.task_done()
            
            except Exception as e:
                logger.error(f"Worker {worker_id}: Unexpected error: {str(e)}")
                queue.task_done()
    
    logger.info(f"Worker {worker_id} stopped. Processed {processed} items")


async def start_workers(
    num_workers: int,
    queue: asyncio.Queue,
    aggregates: Aggregates,
    output_file_path: str
) -> list:
    """
    Start a pool of worker coroutines.
    
    Args:
        num_workers: Number of workers to start
        queue: Shared queue
        aggregates: Shared aggregates
        output_file_path: Output file path
    
    Returns:
        List of worker tasks
    """
    # Clear/create output file
    with open(output_file_path, 'w') as f:
        pass  # Empty the file
    
    # Start workers
    tasks = []
    for worker_id in range(num_workers):
        task = asyncio.create_task(
            worker(worker_id, queue, aggregates, output_file_path)
        )
        tasks.append(task)
    
    logger.info(f"Started {num_workers} workers")
    return tasks


async def stop_workers(queue: asyncio.Queue, worker_tasks: list) -> None:
    """
    Gracefully stop all workers.
    
    Args:
        queue: Shared queue
        worker_tasks: List of worker tasks
    """
    # Send poison pill to each worker
    for _ in worker_tasks:
        await queue.put(None)
    
    # Wait for all workers to finish
    await asyncio.gather(*worker_tasks)
    
    logger.info("All workers stopped")