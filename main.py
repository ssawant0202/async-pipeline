"""
Main pipeline orchestrator.
Coordinates fetchers, workers, and produces final summary.
"""
import asyncio
import json
import logging
import time
from typing import Dict, Any
import httpx

from fetcher import EndpointConfig, fetch_endpoint
from processor import Aggregates, start_workers, stop_workers

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Endpoint configurations
ENDPOINTS = [
    EndpointConfig(
        name='jsonplaceholder',
        url='https://jsonplaceholder.typicode.com/posts',
        rate_limit=5
    ),
    EndpointConfig(
        name='dummyjson',
        url='https://dummyjson.com/products',
        rate_limit=5
    ),
    EndpointConfig(
        name='escuelajs',
        url='https://api.escuelajs.co/api/v1/products',
        rate_limit=5
    ),
]


async def run_pipeline(
    endpoints: list,
    num_workers: int = 8,
    queue_size: int = 500,
    output_file: str = 'products.ndjson',
    summary_file: str = 'summary.json'
) -> Dict[str, Any]:
    """
    Main pipeline execution.
    
    Pipeline flow:
    1. Start worker pool
    2. Start fetchers for each endpoint (concurrent)
    3. Fetchers put items into queue
    4. Workers process items from queue
    5. Workers write to NDJSON file and update aggregates
    6. Wait for all fetchers to complete
    7. Stop workers gracefully
    8. Generate summary
    
    Args:
        endpoints: List of EndpointConfig objects
        num_workers: Number of processing workers
        queue_size: Max queue size (for backpressure)
        output_file: Output NDJSON file path
        summary_file: Summary JSON file path
    
    Returns:
        Summary dictionary
    """
    start_time = time.time()
    
    # Create queue with bounded size for backpressure
    queue = asyncio.Queue(maxsize=queue_size)
    
    # Initialize metrics
    metrics = {
        'endpoints': {},
        'start_time': start_time
    }
    
    # Initialize aggregates
    aggregates = Aggregates()
    
    logger.info("=" * 60)
    logger.info("Starting Async Data Pipeline")
    logger.info(f"Endpoints: {len(endpoints)}")
    logger.info(f"Workers: {num_workers}")
    logger.info(f"Queue size: {queue_size}")
    logger.info("=" * 60)
    
    # Create HTTP client (reused across all fetchers)
    async with httpx.AsyncClient() as client:
        # Start workers
        worker_tasks = await start_workers(
            num_workers=num_workers,
            queue=queue,
            aggregates=aggregates,
            output_file_path=output_file
        )
        
        # Start fetchers (one per endpoint, all concurrent)
        fetcher_tasks = []
        for endpoint in endpoints:
            task = asyncio.create_task(
                fetch_endpoint(
                    config=endpoint,
                    queue=queue,
                    metrics=metrics,
                    client=client
                )
            )
            fetcher_tasks.append(task)
        
        # Wait for all fetchers to complete
        logger.info("Waiting for fetchers to complete...")
        await asyncio.gather(*fetcher_tasks, return_exceptions=True)
        logger.info("All fetchers completed")
        
        # Wait for queue to be processed
        logger.info("Waiting for queue to be processed...")
        await queue.join()
        logger.info("Queue processing complete")
        
        # Stop workers
        await stop_workers(queue, worker_tasks)
    
    end_time = time.time()
    processing_time = round(end_time - start_time, 2)
    
    # Build summary
    summary = build_summary(
        aggregates=aggregates,
        metrics=metrics,
        processing_time=processing_time,
        output_file=output_file
    )
    
    # Write summary to file
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info("=" * 60)
    logger.info("Pipeline Complete!")
    logger.info(f"Total products: {summary['total_products']}")
    logger.info(f"Processing time: {processing_time}s")
    logger.info(f"Success rate: {summary['success_rate']}%")
    logger.info(f"Output: {output_file}")
    logger.info(f"Summary: {summary_file}")
    logger.info("=" * 60)
    
    return summary


def build_summary(
    aggregates: Aggregates,
    metrics: Dict[str, Any],
    processing_time: float,
    output_file: str
) -> Dict[str, Any]:
    """
    Build final summary report.
    
    Args:
        aggregates: Aggregates object with statistics
        metrics: Metrics dictionary from fetchers
        processing_time: Total processing time in seconds
        output_file: Output file path
    
    Returns:
        Summary dictionary
    """
    # Calculate success rate
    total_requests = sum(
        m['requests_sent'] for m in metrics['endpoints'].values()
    )
    total_failures = sum(
        m['requests_failed'] for m in metrics['endpoints'].values()
    )
    
    if total_requests > 0:
        success_rate = round(
            ((total_requests - total_failures) / total_requests) * 100,
            2
        )
    else:
        success_rate = 0.0
    
    # Build summary
    summary = {
        'total_products': aggregates.total_products,
        'processing_time_seconds': processing_time,
        'success_rate': success_rate,
        'output_file': output_file,
        'statistics': aggregates.to_dict(),
        'endpoints': metrics['endpoints'],
        'errors': {
            'failed_requests': total_failures,
            'failed_normalizations': aggregates.failed_normalizations
        }
    }
    
    return summary


async def main():
    """Entry point."""
    try:
        summary = await run_pipeline(
            endpoints=ENDPOINTS,
            num_workers=8,
            queue_size=500
        )
        
        # Print summary to console
        print("\n" + "=" * 60)
        print("PIPELINE SUMMARY")
        print("=" * 60)
        print(json.dumps(summary, indent=2))
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise


if __name__ == '__main__':
    # Run the async main function
    asyncio.run(main())