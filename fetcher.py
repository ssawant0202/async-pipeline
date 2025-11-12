"""
Async fetcher with rate limiting, retries, and error handling.
Each endpoint is fetched concurrently with pagination support.
"""
import asyncio
import logging
from typing import Dict, Any, List
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)


class EndpointConfig:
    """Configuration for a single API endpoint."""
    def __init__(self, name: str, url: str, rate_limit: int = 5):
        self.name = name
        self.url = url
        self.rate_limit = rate_limit  # Max requests per second
        self.semaphore = asyncio.Semaphore(rate_limit) # Unnecessary Sem, remove


class RateLimiter:
    """
    Simple rate limiter using semaphore + sleep pattern.
    Allows N requests, then sleeps for 1 second.
    """
    def __init__(self, requests_per_second: int):
        self.requests_per_second = requests_per_second
        self.semaphore = asyncio.Semaphore(requests_per_second)
        self.request_count = 0
        self.last_reset = asyncio.get_event_loop().time()
    
    async def acquire(self):
        """Wait if we've hit the rate limit."""
        await self.semaphore.acquire()
        
        current_time = asyncio.get_event_loop().time()
        
        # Reset counter every second
        if current_time - self.last_reset >= 1.0:
            self.request_count = 0
            self.last_reset = current_time
        
        self.request_count += 1
        
        # If All of the 5 sem are busy sleep until next second
        if self.request_count >= self.requests_per_second:
            sleep_time = 1.0 - (current_time - self.last_reset)
            if sleep_time > 0: # Deal with negative values if occours
                await asyncio.sleep(sleep_time)
    
    def release(self):
        # Release the semaphore
        self.semaphore.release()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    reraise=True
)
async def fetch_with_retry(client: httpx.AsyncClient, url: str) -> Dict[str, Any]:
    """
    Fetch a URL with automatic retries on failure.
    Retries up to 3 times with exponential backoff: 0.5s, 1s, 2s
    
    Args:
        client: httpx async client
        url: URL to fetch
    
    Returns:
        JSON response as dictionary
    
    Raises:
        httpx.HTTPError: If all retries fail
    """
    logger.debug(f"Fetching: {url}")
    response = await client.get(url, timeout=10.0)
    response.raise_for_status()
    return response.json()


async def fetch_endpoint(
    config: EndpointConfig,
    queue: asyncio.Queue,
    metrics: Dict[str, Any],
    client: httpx.AsyncClient
) -> None:
    """
    Fetch all pages from a single endpoint and put items into queue.
    
    This function:
    1. Respects rate limits (5 req/sec per endpoint)
    2. Handles pagination automatically
    3. Retries failed requests with exponential backoff
    4. Tracks success/failure metrics
    
    Args:
        config: Endpoint configuration
        queue: Asyncio queue to put fetched items
        metrics: Shared metrics dictionary
        client: HTTP client instance
    """
    rate_limiter = RateLimiter(config.rate_limit)
    page = 1
    total_items = 0
    consecutive_failures = 0
    max_consecutive_failures = 5
    
    logger.info(f"Starting fetcher for {config.name}")
    
    # Initialize endpoint metrics
    if config.name not in metrics['endpoints']:
        metrics['endpoints'][config.name] = { # Handle incorrect api call summary
            'requests_sent': 0,
            'requests_failed': 0,
            'items_fetched': 0
        }
    
    while True:
        # Simple circuit breaker: stop after too many consecutive failures
        if consecutive_failures >= max_consecutive_failures:
            logger.error(f"{config.name}: Too many failures, stopping fetcher")
            break
        
        try:
            # Rate limiting
            await rate_limiter.acquire()
            
            
            # Different pagination URI's for different API endpoints
            if 'jsonplaceholder' in config.url:
                url = f"{config.url}?_page={page}&_limit=10"
            elif 'dummyjson' in config.url:
                skip = (page - 1) * 10
                url = f"{config.url}?limit=10&skip={skip}"
            else:  # escuelajs
                offset = (page - 1) * 10
                url = f"{config.url}?offset={offset}&limit=10"
            
            # Fetch with automatic retries
            data = await fetch_with_retry(client, url)
            
            # Update metrics
            metrics['endpoints'][config.name]['requests_sent'] += 1
            consecutive_failures = 0  # Reset on success
            
            # Extract items (different APIs have different structures)
            items = extract_items(data, config.name)
            
            if not items:
                logger.info(f"{config.name}: No more items, stopping at page {page}")
                break
            
            # Put each item into the queue for processing
            for item in items:
                await queue.put({
                    'source': config.name,
                    'payload': item
                })
                total_items += 1
            
            metrics['endpoints'][config.name]['items_fetched'] += len(items)
            logger.debug(f"{config.name}: Fetched {len(items)} items from page {page}")
            
            page += 1
            
            # Safety limit: don't fetch more than 100 pages
            if page > 100:
                logger.warning(f"{config.name}: Reached page limit, stopping")
                break
                
        except Exception as e:
            consecutive_failures += 1
            metrics['endpoints'][config.name]['requests_failed'] += 1
            logger.error(f"{config.name}: Error on page {page}: {str(e)}")
            
            # If we're getting errors on page 1, stop immediately
            if page == 1 and consecutive_failures >= 3:
                logger.error(f"{config.name}: Failed to fetch first page, stopping")
                break
            
            page += 1  # Try next page
        
        finally:
            rate_limiter.release()
    
    logger.info(f"{config.name}: Fetcher completed. Total items: {total_items}")


def extract_items(data: Any, source: str) -> List[Dict]:
    """
    Extract items from API response.
    Different APIs return data in different formats.
    
    Args:
        data: Raw API response
        source: API source name
    
    Returns:
        List of item dictionaries
    """
    if source == 'jsonplaceholder':
        # JSONPlaceholder returns array directly
        return data if isinstance(data, list) else []
    
    elif source == 'dummyjson':
        # DummyJSON wraps in {"products": [...]}
        return data.get('products', []) if isinstance(data, dict) else []
    
    elif source == 'escuelajs':
        # Escuela JS returns array directly
        return data if isinstance(data, list) else []
    
    else:
        # Fallback
        return data if isinstance(data, list) else []