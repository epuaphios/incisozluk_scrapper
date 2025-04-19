import sys
import logging
import asyncio
import random
from typing import Optional, Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import ClientSession, TCPConnector

MONGO_URI = "mongodb://scraper:scraper123@192.168.1.117:27017/incisozluk?authSource=incisozluk"
DB_NAME = "incisozluk"
COLLECTION_NAME = "entries"

CONCURRENT_TASKS = 250  # Eşzamanlı request sayısı
REQUEST_TIMEOUT = 25
MAX_RETRIES = 15
BASE_DELAY = 0.3
ENTRY_START = 11729109
ENTRY_END = 20000000

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/111.0.1661.62',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0'
]

logger = logging.getLogger('InciScraper')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_text(text: str) -> str:
    return text.strip()

def generate_random_cookies():
    return {}

async def parse_html(html: str, entry_id: int) -> Optional[Dict[str, Any]]:
    return {"entry_id": entry_id, "content": html}

async def get_wiki_first_comment(slug: str) -> str:
    return "First comment"

class InciScraper:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(CONCURRENT_TASKS)
        self.rate_limiter = RateLimiter(requests_per_second=100)
        self.client = AsyncIOMotorClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.collection = self.db[COLLECTION_NAME]

    async def fetch_entry(self, session: ClientSession, entry_id: int) -> Optional[Dict]:
        url = f"https://incisozluk.co/e/{entry_id}"
        headers = {'User-Agent': random.choice(USER_AGENTS)}
        cookies = generate_random_cookies()
        async with self.semaphore, self.rate_limiter:
            for attempt in range(MAX_RETRIES):
                try:
                    async with session.get(url, headers=headers, cookies=cookies, timeout=REQUEST_TIMEOUT) as response:
                        response.raise_for_status()
                        html = await response.text()
                        parsed_data = await parse_html(html, entry_id)
                        await self.collection.update_one(
                            {"entry_id": entry_id},
                            {"$set": parsed_data},
                            upsert=True
                        )
                        logger.info(f"Processed #{entry_id}")
                        return parsed_data
                except Exception as e:
                    logger.error(f"Failed to fetch entry {entry_id} on attempt {attempt + 1}: {e}")
                    await asyncio.sleep(BASE_DELAY * (2 ** attempt))
        return None

    async def run(self):
        connector = TCPConnector(limit=0, ssl=False)
        async with ClientSession(connector=connector) as session:
            tasks = [self.fetch_entry(session, entry_id) for entry_id in range(ENTRY_START, ENTRY_END)]
            await asyncio.gather(*tasks)

class RateLimiter:
    """Dinamik rate limiter"""
    def __init__(self, requests_per_second: int):
        self.requests_per_second = requests_per_second
        self.tokens = requests_per_second
        self.updated_at = asyncio.get_event_loop().time()

    async def __aenter__(self):
        await self._wait_for_token()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def _wait_for_token(self):
        while self.tokens == 0:
            await asyncio.sleep(1 / self.requests_per_second)
        self.tokens -= 1
        self._add_tokens()

    def _add_tokens(self):
        now = asyncio.get_event_loop().time()
        elapsed = now - self.updated_at
        self.tokens = min(self.tokens + int(elapsed * self.requests_per_second), self.requests_per_second)
        self.updated_at = now

if __name__ == "__main__":
    scraper = InciScraper()
    try:
        asyncio.run(scraper.run())
    except KeyboardInterrupt:
        logger.info("Scraper stopped by user")
    finally:
        scraper.client.close()
