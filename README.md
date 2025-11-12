# Async Data Processing Pipeline

A production-ready concurrent data processing pipeline built with Python asyncio that fetches, processes, and stores product data from multiple REST APIs with comprehensive error handling, rate limiting, and retry logic.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Resilience Features](#resilience-features)
- [Output](#output)
- [Performance](#performance)
- [Design Decisions](#design-decisions)
- [Limitations & Trade-offs](#limitations--trade-offs)
- [Future Improvements](#future-improvements)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

---

## Overview

This project implements a scalable async data pipeline that demonstrates modern Python concurrency patterns for data engineering tasks. It fetches product data from three different REST APIs concurrently, normalizes the data to a canonical schema, and streams results to disk while maintaining comprehensive metrics.

**Target Processing Time:** ~30-60 seconds for ~200-300 products  
**Concurrency Level:** 3 concurrent API fetchers + 8 processing workers  
**Memory Efficiency:** Streaming output, bounded queue prevents overflow  

### Key Objectives

- ✅ Demonstrate async/await patterns for I/O-bound workloads
- ✅ Implement producer-consumer architecture with backpressure
- ✅ Handle failures gracefully (retries, circuit breaker, logging)
- ✅ Process data at scale without memory issues
- ✅ Generate comprehensive metrics and summaries

---

## Features

### Core Functionality

✅ **Multi-API Concurrent Fetching**
- Fetch from 3 different APIs simultaneously
- Independent rate limiting per endpoint (5 req/sec)
- Automatic pagination handling

✅ **Robust Error Handling**
- Automatic retries with exponential backoff (3 attempts)
- Simple circuit breaker (stops after 5 consecutive failures)
- Per-request and per-endpoint error tracking
- Graceful degradation (partial results on failure)

✅ **Efficient Data Processing**
- Queue-based producer-consumer pattern
- 8 concurrent workers for processing
- Bounded queue (500 items) for backpressure
- Streaming NDJSON output (memory efficient)

✅ **Data Normalization**
- Transform 3 different API formats → canonical schema
- Handle nested structures and missing fields
- Type conversion and validation

✅ **Comprehensive Metrics**
- Success/failure rates per endpoint
- Products by category and source
- Average prices by category
- Processing time and throughput

---

## Architecture

### High-Level Design

```
┌────────────────────────────────────────────────────────┐
│                      main.py                            │
│              (Pipeline Orchestrator)                    │
│  • Creates queue (max 500 items)                        │
│  • Starts 3 fetchers + 8 workers                        │
│  • Collects metrics & generates summary                 │
└─────┬──────────────────────────────────────────────────┘
      │
      ├──────────────────────────────────────────┐
      │                                          │
      ▼                                          ▼
┌─────────────────┐                    ┌─────────────────┐
│  fetcher.py     │                    │  processor.py    │
│  (3 instances)  │                    │  (8 workers)     │
│                 │                    │                  │
│ • Rate limiting │─────[Queue]───────▶│ • Normalize data │
│ • Retries (3x)  │   (bounded 500)    │ • Update metrics │
│ • Pagination    │                    │ • Write NDJSON   │
│ • Circuit break │                    │                  │
└─────────────────┘                    └─────────────────┘
         │                                      │
         │ Uses                                 │ Uses
         ▼                                      ▼
┌─────────────────┐                    ┌─────────────────┐
│  httpx          │                    │  schema.py      │
│  • Async HTTP   │                    │  • 3 normalizers│
│  • Timeout: 10s │                    │  • Canonical    │
└─────────────────┘                    │    format       │
                                       └─────────────────┘
```

### Data Flow (Step by Step)

```
1. main.py creates asyncio.Queue(maxsize=500)
                    │
                    ▼
2. Start 3 fetcher tasks (one per API endpoint)
   ┌──────────────┬──────────────┬──────────────┐
   │ JSONPlaceholder│  DummyJSON │  Escuela.js  │
   │    Fetcher    │   Fetcher   │   Fetcher    │
   └───────┬────────┴──────┬──────┴──────┬───────┘
           │               │              │
           └───────────────┼──────────────┘
                          │
                          ▼
            [asyncio.Queue - Max 500 items]
            • Provides backpressure
            • Pauses fetchers if full
                          │
                          ▼
   ┌──────────────────────────────────────────┐
   │  8 Worker Tasks (concurrent processors)  │
   │  Each worker:                             │
   │  1. Gets item from queue                  │
   │  2. Normalizes using schema.py            │
   │  3. Writes to products.ndjson (streaming) │
   │  4. Updates shared aggregates             │
   └──────────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────┐
        │  Output Files Generated:     │
        │  • products.ndjson (stream)  │
        │  • summary.json (final)      │
        └─────────────────────────────┘
```

### Concurrency Model

**3 Fetchers Run Concurrently:**
```python
# All 3 start at the same time
fetcher_tasks = [
    asyncio.create_task(fetch_endpoint(config1, queue, ...)),
    asyncio.create_task(fetch_endpoint(config2, queue, ...)),
    asyncio.create_task(fetch_endpoint(config3, queue, ...))
]

# They don't wait for each other
# Each fetches at its own pace (up to 5 req/sec per endpoint)
```

**8 Workers Process Concurrently:**
```python
# All 8 workers compete for items from queue
workers = [
    asyncio.create_task(worker(i, queue, ...))
    for i in range(8)
]

# Worker that finishes first gets next item
# Natural load balancing
```

**Queue Provides Backpressure:**
```python
# If queue is full (500 items):
await queue.put(item)  # Fetcher pauses here until space available

# If processors are faster than fetchers:
item = await queue.get()  # Worker waits here until item available
```

---

## Quick Start

### Prerequisites

- **Python 3.8+** (tested with 3.11)
- **pip** package manager
- **Internet connection** (fetches from public APIs)

### Installation

```bash
# 1. Clone repository
git clone <your-repo-url>
cd async-pipeline

# 2. Create virtual environment (recommended)
python -m venv .venv

# 3. Activate virtual environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate

# 4. Install dependencies
pip install -r requirements.txt
```

**Dependencies installed:**
- `httpx` - Async HTTP client
- `tenacity` - Retry logic with exponential backoff
- `pytest` - Testing framework
- `pytest-asyncio` - Async test support

### Running the Pipeline

```bash
# Run the full pipeline
python main.py
```

### Expected Output

```
============================================================
Starting Async Data Pipeline
Endpoints: 3
Workers: 8
Queue size: 500
============================================================
INFO - Started 8 workers
INFO - Starting fetcher for jsonplaceholder
INFO - Starting fetcher for dummyjson
INFO - Starting fetcher for escuelajs
INFO - Waiting for fetchers to complete...
DEBUG - dummyjson: Fetched 10 items from page 1
DEBUG - escuelajs: Fetched 10 items from page 1
DEBUG - jsonplaceholder: Fetched 10 items from page 1
...
INFO - All fetchers completed
INFO - Waiting for queue to be processed...
INFO - Queue processing complete
INFO - All workers stopped
============================================================
Pipeline Complete!
Total products: 250
Processing time: 45.32s
Success rate: 98.5%
Output: products.ndjson
Summary: summary.json
============================================================
```

### Verifying Results

```bash
# Check output files
ls -lh products.ndjson summary.json

# Count products processed
wc -l products.ndjson

# View first 5 products
head -n 5 products.ndjson

# View summary statistics
cat summary.json | python -m json.tool

# Check for errors
grep "ERROR" pipeline.log  # If logging to file
```

---

## How It Works

### 1. Pipeline Initialization (main.py)

```python
async def run_pipeline():
    # Create bounded queue for backpressure
    queue = asyncio.Queue(maxsize=500)
    
    # Initialize metrics tracking
    metrics = {'endpoints': {}}
    
    # Initialize aggregates (stats collector)
    aggregates = Aggregates()
    
    # Create shared HTTP client
    async with httpx.AsyncClient() as client:
        # Start processing workers first
        worker_tasks = await start_workers(
            num_workers=8,
            queue=queue,
            aggregates=aggregates,
            output_file_path='products.ndjson'
        )
        
        # Start all fetchers concurrently
        fetcher_tasks = []
        for endpoint_config in ENDPOINTS:
            task = asyncio.create_task(
                fetch_endpoint(
                    config=endpoint_config,
                    queue=queue,
                    metrics=metrics,
                    client=client
                )
            )
            fetcher_tasks.append(task)
        
        # Wait for all fetchers to complete
        await asyncio.gather(*fetcher_tasks, return_exceptions=True)
        
        # Wait for queue to empty
        await queue.join()
        
        # Stop workers gracefully
        await stop_workers(queue, worker_tasks)
```

**Key Points:**
- Workers start BEFORE fetchers (ready to process immediately)
- All 3 fetchers run concurrently via `create_task()`
- `gather()` waits for all fetchers (even if some fail)
- `queue.join()` waits for all items to be processed
- Graceful shutdown with poison pills

---

### 2. Async Fetching with Resilience (fetcher.py)

```python
async def fetch_endpoint(config, queue, metrics, client):
    """
    Fetch all pages from one endpoint.
    Handles: rate limiting, retries, pagination, circuit breaking
    """
    rate_limiter = RateLimiter(config.rate_limit)  # 5 req/sec
    page = 1
    consecutive_failures = 0
    max_failures = 5  # Simple circuit breaker
    
    while True:
        # Circuit breaker: stop if too many failures
        if consecutive_failures >= max_failures:
            logger.error(f"{config.name}: Too many failures, stopping")
            break
        
        try:
            # Rate limiting (wait if needed)
            await rate_limiter.acquire()
            
            # Build paginated URL (different per API)
            url = build_url(config.url, page)
            
            # Fetch with automatic retries (tenacity decorator)
            data = await fetch_with_retry(client, url)
            
            # Reset failure counter on success
            consecutive_failures = 0
            metrics['endpoints'][config.name]['requests_sent'] += 1
            
            # Extract items (different structure per API)
            items = extract_items(data, config.name)
            
            if not items:
                break  # No more pages
            
            # Put items in queue for processing
            for item in items:
                await queue.put({
                    'source': config.name,
                    'payload': item
                })
            
            page += 1
            
        except Exception as e:
            consecutive_failures += 1
            metrics['endpoints'][config.name]['requests_failed'] += 1
            logger.error(f"{config.name}: Error on page {page}: {e}")
            
        finally:
            rate_limiter.release()
```

**Retry Logic (tenacity decorator):**
```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    reraise=True
)
async def fetch_with_retry(client, url):
    response = await client.get(url, timeout=10.0)
    response.raise_for_status()
    return response.json()
```

**Retry Schedule:**
- Attempt 1: Immediate
- Attempt 2: Wait 0.5s
- Attempt 3: Wait 1s
- Attempt 4: Fails and raises exception

**Rate Limiting (RateLimiter class):**
```python
class RateLimiter:
    """Allow N requests per second using semaphore + time window"""
    
    async def acquire(self):
        await self.semaphore.acquire()
        
        current_time = asyncio.get_event_loop().time()
        
        # Reset counter every second
        if current_time - self.last_reset >= 1.0:
            self.request_count = 0
            self.last_reset = current_time
        
        self.request_count += 1
        
        # If we've used all tokens, sleep until next second
        if self.request_count >= self.requests_per_second:
            sleep_time = 1.0 - (current_time - self.last_reset)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
```

---

### 3. Worker Pool Processing (processor.py)

```python
async def worker(worker_id, queue, aggregates, output_file_path):
    """
    Single worker: gets items from queue, processes, writes to file
    """
    with open(output_file_path, 'a') as f:  # Append mode
        while True:
            # Get next item (waits if queue empty)
            item = await queue.get()
            
            # Check for poison pill (shutdown signal)
            if item is None:
                queue.task_done()
                break
            
            try:
                source = item['source']
                raw_payload = item['payload']
                
                # Normalize to canonical format
                product = normalize_product(source, raw_payload)
                
                # Update shared aggregates (in-memory stats)
                aggregates.add_product(product)
                
                # Write to file (streaming)
                json_line = json.dumps(asdict(product))
                f.write(json_line + '\n')
                
            except Exception as e:
                aggregates.add_failure()
                logger.error(f"Worker {worker_id}: Error: {e}")
            
            finally:
                # Always mark task as done (important!)
                queue.task_done()
```

**Why 8 workers?**
- Balances concurrency without overwhelming CPU
- Rule of thumb: `min(32, CPU_count * 4)` for I/O-bound tasks
- 8 is sweet spot for this workload

**Why append mode ('a') is safe:**
- Python's GIL ensures `file.write()` is atomic for small writes
- Each write is a single line (no interleaving)
- Alternative would be locks (slower, unnecessary here)

---

### 4. Data Normalization (schema.py)

```python
@dataclass
class Product:
    """Canonical product schema - all sources map to this"""
    id: str
    title: str
    source: str
    price: float
    currency: str
    category: str
    processed_at: str


def normalize_dummyjson(raw_item):
    """
    DummyJSON format:
    {"id": 1, "title": "iPhone", "price": 549, "category": "smartphones"}
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


def normalize_escuelajs(raw_item):
    """
    Escuela JS format (nested category):
    {"id": 1, "title": "Product", "price": 100, 
     "category": {"name": "Clothes", "id": 2}}
    """
    # Handle nested structure
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


# Dispatcher function
def normalize_product(source, raw_item):
    """Route to correct normalizer based on source"""
    normalizers = {
        'jsonplaceholder': normalize_jsonplaceholder,
        'dummyjson': normalize_dummyjson,
        'escuelajs': normalize_escuelajs,
    }
    
    normalizer = normalizers.get(source)
    if not normalizer:
        raise ValueError(f"Unknown source: {source}")
    
    return normalizer(raw_item)
```

---

### 5. Aggregates & Statistics (processor.py)

```python
class Aggregates:
    """Track statistics during processing"""
    
    def __init__(self):
        self.total_products = 0
        self.failed_normalizations = 0
        self.category_counts = defaultdict(int)
        self.price_sum_by_category = defaultdict(float)
        self.price_count_by_category = defaultdict(int)
        self.source_counts = defaultdict(int)
    
    def add_product(self, product):
        """Update stats with new product"""
        self.total_products += 1
        self.category_counts[product.category] += 1
        self.source_counts[product.source] += 1
        
        if product.price > 0:
            self.price_sum_by_category[product.category] += product.price
            self.price_count_by_category[product.category] += 1
    
    def get_average_prices(self):
        """Calculate average price per category"""
        averages = {}
        for category in self.price_sum_by_category:
            count = self.price_count_by_category[category]
            if count > 0:
                averages[category] = round(
                    self.price_sum_by_category[category] / count,
                    2
                )
        return averages
```

---

## Project Structure

```
async-pipeline/
│
├── main.py                 # Pipeline orchestrator (entry point)
│   • run_pipeline() - main async function
│   • build_summary() - generate final report
│   • ENDPOINTS configuration
│
├── fetcher.py             # Async data fetching
│   • fetch_endpoint() - fetch all pages from one API
│   • fetch_with_retry() - HTTP request with retries
│   • RateLimiter - simple rate limiting class
│   • extract_items() - parse different API responses
│
├── processor.py           # Data processing workers
│   • worker() - single worker coroutine
│   • start_workers() - create worker pool
│   • stop_workers() - graceful shutdown
│   • Aggregates - statistics tracking class
│
├── schema.py              # Data models & normalization
│   • Product dataclass - canonical schema
│   • normalize_jsonplaceholder() - mapper
│   • normalize_dummyjson() - mapper
│   • normalize_escuelajs() - mapper
│   • normalize_product() - dispatcher
│
├── test_pipeline.py       # Integration tests
│   • TestNormalization - unit tests for mappers
│   • TestPipeline - end-to-end tests
│
├── requirements.txt       # Python dependencies
├── README.md             # This file
├── AI_USAGE.md           # AI assistance documentation
│
├── products.ndjson       # Output: normalized products (generated)
└── summary.json          # Output: statistics (generated)
```

### File Statistics

| File | Lines | Purpose | Complexity |
|------|-------|---------|------------|
| `main.py` | ~200 | Orchestration | ⭐⭐ Medium |
| `fetcher.py` | ~180 | Fetching logic | ⭐⭐⭐ High |
| `processor.py` | ~150 | Processing | ⭐⭐ Medium |
| `schema.py` | ~100 | Data models | ⭐ Low |
| `test_pipeline.py` | ~120 | Tests | ⭐⭐ Medium |
| **Total** | **~750** | | |

---

## Configuration

### Endpoint Configuration (main.py)

```python
ENDPOINTS = [
    EndpointConfig(
        name='jsonplaceholder',
        url='https://jsonplaceholder.typicode.com/posts',
        rate_limit=5  # Max 5 requests per second
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
```

**To add a new endpoint:**
1. Add `EndpointConfig` to list
2. Implement normalizer in `schema.py`
3. Add to `NORMALIZERS` dict
4. Update `extract_items()` if needed

### Pipeline Parameters (main.py)

```python
# Worker configuration
NUM_WORKERS = 8           # Concurrent processors
QUEUE_SIZE = 500          # Max items in queue (backpressure)

# Output files
OUTPUT_FILE = 'products.ndjson'
SUMMARY_FILE = 'summary.json'

# Retry configuration (fetcher.py)
MAX_RETRIES = 3
RETRY_MULTIPLIER = 0.5    # Exponential backoff: 0.5s, 1s, 2s

# Rate limiting (per endpoint)
REQUESTS_PER_SECOND = 5

# Request timeout
HTTP_TIMEOUT = 10.0       # seconds

# Circuit breaker
MAX_CONSECUTIVE_FAILURES = 5
```

### Environment Variables (Optional)

```bash
# Set custom configuration
export PIPELINE_WORKERS=16
export PIPELINE_QUEUE_SIZE=1000
export PIPELINE_RATE_LIMIT=10

python main.py
```

---

## Resilience Features

### 1. Automatic Retries with Exponential Backoff

**Implementation:** `tenacity` library decorator

**Configuration:**
- Max attempts: 3
- Backoff: exponential with multiplier 0.5
- Retry on: `HTTPError`, `TimeoutException`
- Max backoff: 4 seconds

**Example:**
```
Request fails → Wait 0.5s → Retry
Still fails   → Wait 1.0s → Retry  
Still fails   → Wait 2.0s → Retry
Still fails   → Raise exception
```

**Why exponential backoff?**
- Gives server time to recover
- Avoids thundering herd problem
- Industry standard pattern

### 2. Per-Endpoint Rate Limiting

**Implementation:** Semaphore + time window

**How it works:**
```python
# Allows burst of 5 requests
# Then enforces 1-second wait
rate_limiter = RateLimiter(5)  # 5 req/sec

await rate_limiter.acquire()  # May wait here
# Make request
rate_limiter.release()
```

**Why per-endpoint?**
- APIs have independent rate limits
- Prevents one slow API from blocking others
- Fair resource allocation

### 3. Simple Circuit Breaker

**Implementation:** Failure counter

**How it works:**
```python
consecutive_failures = 0

try:
    data = await fetch()
    consecutive_failures = 0  # Reset on success
except:
    consecutive_failures += 1
    if consecutive_failures >= 5:
        logger.error("Too many failures, stopping")
        break  # Stop fetching from this endpoint
```

**Why simple version?**
- Full 3-state circuit breaker is complex (CLOSED/OPEN/HALF_OPEN)
- For this scope, simple counting is sufficient
- Prevents wasting time on dead endpoints

**Trade-off:** No automatic recovery after cooldown

### 4. Backpressure via Bounded Queue

**Implementation:** `asyncio.Queue(maxsize=500)`

**How it works:**
```python
# Fetcher side
await queue.put(item)  # Pauses here if queue is full (500 items)

# Processor side
item = await queue.get()  # Waits here if queue is empty
```

**Why bounded?**
- Prevents memory overflow if fetchers are faster than processors
- Natural flow control (fetchers automatically slow down)
- No complex coordination needed

### 5. Graceful Error Handling

**Levels of error handling:**

```python
# Level 1: Per-request (automatic retries)
@retry(stop=stop_after_attempt(3))
async def fetch():
    ...

# Level 2: Per-page (log and continue)
try:
    data = await fetch_page(page)
except Exception as e:
    logger.error(f"Page {page} failed: {e}")
    continue  # Try next page

# Level 3: Per-endpoint (circuit breaker)
if consecutive_failures >= 5:
    logger.error("Endpoint dead, stopping")
    break

# Level 4: Per-item (normalization)
try:
    product = normalize(item)
except Exception as e:
    aggregates.add_failure()
    logger.error(f"Normalization failed: {e}")
    # Don't crash, just skip this item
```

**Result:** Partial success is better than total failure

---

## Output

### products.ndjson (Newline-Delimited JSON)

One JSON object per line (not a valid JSON array):

```json
{"id": "dummyjson_1", "title": "iPhone 9", "source": "dummyjson", "price": 549.0, "currency": "USD", "category": "smartphones", "processed_at": "2024-11-11T10:30:00.123456"}
{"id": "escuelajs_42", "title": "Awesome Product", "source": "escuelajs", "price": 99.99, "currency": "USD", "category": "Electronics", "processed_at": "2024-11-11T10:30:00.456789"}
{"id": "jsonplaceholder_5", "title": "Sample Post", "source": "jsonplaceholder", "price": 9.99, "currency": "USD", "category": "post", "processed_at": "2024-11-11T10:30:00.789012"}
```

**Advantages of NDJSON:**
- ✅ **Streaming:** Write line-by-line as data arrives
- ✅ **Memory efficient:** No need to hold all data in RAM
- ✅ **Fault tolerant:** Partial results saved if crash
- ✅ **Easy processing:** `grep`, `awk`, `jq` work directly
- ✅ **Append-safe:** Multiple workers can append concurrently

**Processing NDJSON:**
```bash
# Count products
wc -l products.ndjson

# Filter by source
cat products.ndjson | jq 'select(.source == "dummyjson")'

# Get all titles
cat products.ndjson | jq -r '.title'

# Calculate average price
cat products.ndjson | jq '.price' | awk '{sum+=$1} END {print sum/NR}'
```

### summary.json

Comprehensive statistics report:

```json
{
  "total_products": 250,
  "processing_time_seconds": 45.32,
  "success_rate": 98.5,
  "output_file": "products.ndjson",
  "statistics": {
    "total_products": 250,
    "failed_normalizations": 2,
    "products_by_category": {
      "smartphones": 50,
      "laptops": 30,
      "Electronics": 45,
      "post": 100,
      "uncategorized": 25
    },
    "products_by_source": {
      "dummyjson": 100,
      "escuelajs": 50,
      "jsonplaceholder": 100
    },
    "average_price_by_category": {
      "smartphones": 549.99,
      "laptops": 1299.99,
      "Electronics": 299.50,
      "post": 9.99
    }
  },
  "endpoints": {
    "dummyjson": {
      "requests_sent": 10,
      "requests_failed": 0,
      "items_fetched": 100
    },
    "escuelajs": {
      "requests_sent": 6,
      "requests_failed": 1,
      "items_fetched": 50
    },
    "jsonplaceholder": {
      "requests_sent": 10,
      "requests_failed": 0,
      "items_fetched": 100
    }
  },
  "errors": {
    "failed_requests": 1,
    "failed_normalizations": 2
  }
}
```

---

## Performance

### Benchmarks (Typical Run)

**Hardware:** MacBook Pro M1, 16GB RAM, 100Mbps internet

| Metric | Value |
|--------|-------|
| Total products | ~250-300 |
| Processing time | 30-60 seconds |
| Throughput | ~5-10 products/sec |
| Memory usage | <100MB |
| CPU usage | 10-20% |

**Bottleneck:** API response time (not the pipeline)

### Scalability

**Current capacity:**
- ✅ Handles 3 endpoints × 100 products each = 300 products
- ✅ Queue size: 500 items
- ✅ Worker pool: 8 workers

**How to scale up:**

```python
# More workers (if CPU-bound processing)
NUM_WORKERS = 16

# Larger queue (if memory available)
QUEUE_SIZE = 2000

# Higher rate limits (if APIs allow)
rate_limit=10  # per endpoint

# More endpoints (if needed)
ENDPOINTS.append(EndpointConfig(...))
```

**Limits:**
- Memory: